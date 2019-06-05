import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

# loads all song data from JSON files and inserts records into database
def process_song_file(cur, filepath):
    # open song file
    # Reads all the JSON files in the song_data directory and converts data into a dataframe
    df = pd.read_json(filepath,lines=True)

    # insert song record
    # takes the song_id','title','artist_id','year','duration' from the song plays data frame
    # and converts the record into a list. 
    # the is list is them inserted into the songs table in the sparkify database
    song_data = df[['song_id','title','artist_id','year','duration']].values.tolist()
    cur.execute(song_table_insert, song_data[0])
    
    # insert artist record
    # takes the 'artist_id','artist_name','artist_location','artist_latitude','artist_longitude' 
    # from the song plays data frame and converts the record into a list. 
    # the is list is them inserted into the artist table in the sparkify database
    artist_data = ( 
    df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist())
    cur.execute(artist_table_insert, artist_data[0])


# PRocess the log files and inserts the data into the database.
def process_log_file(cur, filepath):
    # open log file
    # reads the json files into the log file directory and coverts the data into a dataframe
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    # the data frame is filtered to only the records that have song information,
    # the excludes the records that show when usere did any action that did not 
    # play a song from sparkify
    df =  df[df.page == 'NextSong']

    # convert timestamp column to datetime
    # the time stamp in milliseconds is converted into a datetime format
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    # the datetime is further sepereated into the 24 hour time, the hour
    # day, week of year, month, year and week day. 
    # the data is then conveted to a dictionary 
    time_data = (
    (df['ts'],t.dt.hour,t.dt.day,t.dt.weekofyear,t.dt.month,t.dt.year,t.dt.weekday))
    column_labels = ('timestamp','hour','day','week of year','month','year','weekday')
    time_dictionary = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(time_dictionary)
    
    # loop through the dictionary and inserted into the time table
    # in the database
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    # All user data is put into a dataframe and all duplicate data is removed
    user_df = df[['userId','firstName','lastName','gender','level']].drop_duplicates()

    # insert user records
    # the data frame is process and and each record is inserted into the user table
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    # for loop to iterate through the song play data
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        # search the data base to find if any of the song or artist ids are already in
        # the songs or artists tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        # if the ids are found pass then data to the songid and artistid variables
        # if not set songid and artistid to None
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        # the song_plays tables is populated with
        #ts,userId,level,songid,artistid,sessionId,location,userAgent
        songplay_data = ( 
        (row.ts,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent))
        cur.execute(songplay_table_insert, songplay_data)
       
    
def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()