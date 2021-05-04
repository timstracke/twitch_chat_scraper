import twitch
import time
from datetime import date, timedelta

from sqlite3 import Connection as SQLite3Connection
from sqlalchemy import event, create_engine, MetaData, Table, Column
from sqlalchemy.engine import Engine
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import sessionmaker, mapper

app = Flask(__name__)

engine = create_engine('sqlite:///db.sqlite3')

app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///db.sqlite3"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

OAUTH_TOKEN = _________________ 
NICKNAME = ________________



# configure sqlite3 to enforce foreign key contraints
@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, SQLite3Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


db = SQLAlchemy(app)

# Config
EMOTES = ["LULW", "OMEGALUL", "ResidentSleeper", "weirdChamp", "PogO", "Pog", "PogChamp"]
STREAMERS = ["mizkif", "ludwig", "xqcow", "imrosen", "esfandtv", "nmplol", "gmnaroditsky", "maskenissen"]
DEBUG = True

day = date.today()


def print_debug(s):
    if DEBUG:
        print("*** {} ***".format(s))

def contains_word(s, w):
    return (' ' + w + ' ') in (' ' + s + ' ')


# Setting up the table for the emotecounts
columns = list()
columns.append(Column("id", db.Integer, primary_key=True))
for emote in EMOTES:
    columns.append(Column(emote, db.Integer))
metadata = MetaData()
emotecount_table = Table("Emotecount", metadata, *columns)
metadata.create_all(engine)

class Emotecount:
    def __init__(self, count):
        for emote, count in count.items():
            self.__dict__[emote] = count

mapper(Emotecount, emotecount_table)



# Setting up the table for the keys for the emotecounts
columns = list()
columns.append(Column("date", db.Date, primary_key=True))
for streamer in STREAMERS:
    columns.append(Column(streamer, db.Integer))

metadata = MetaData()
countkeys_table  = Table('COUNTKEYS', metadata, *columns)
metadata.create_all(engine) 

class CountKeys:
    def __init__(self, keys, day):
        print(keys)
        self.__dict__["date"] = day
        for streamer, key in keys.items():
            self.__dict__[streamer] = key
            

mapper(CountKeys, countkeys_table)


class Streamscraper():

    def __init__(self, streamer):
        self.streamer = streamer
        self.chat = twitch.Chat(channel=streamer.name, nickname=NICKNAME, oauth=OAUTH_TOKEN).subscribe(
            lambda message: self.process_message(message, streamer))
        print_debug("connected to {}".format(streamer.name))
        
        


    def process_message(self, msg, streamer):
        print("{}: {}".format(streamer.name, msg.text))
        for emote in EMOTES:
            if contains_word(msg.text, emote):
                streamer.count_emote(emote)
                return 


class Streamer():
    def __init__(self, name):
        self.name = name
        self.emotes = {em: 0 for em in EMOTES}

    def count_emote(self, emote):
        self.emotes[emote] += 1
        print(self.emotes)

    def end_count(self):
        print_debug("Ending count for {}".format(self.name))

        new_emotecount = Emotecount(self.emotes)
        db.session.add(new_emotecount)
        
        db.session.commit()
        print_debug("ADDED")
        self.emotes = {em: 0 for em in EMOTES}
        return new_emotecount.id
        




if __name__ == "__main__":
    db.create_all()
    streamer_obj = []
    scraper_obj = []
    for streamer in STREAMERS:
        streamer_obj.append(Streamer(streamer))
        scraper_obj.append(Streamscraper(streamer_obj[-1]))
    while True: 
        if day != date.today():
            print_debug("NEW DAY")
            keys = dict.fromkeys(STREAMERS, 0)
            for streamer in streamer_obj:
                key = streamer.end_count()
                print_debug("Ended count for {}".format(streamer.name))
                keys[streamer.name] = key
            
            new_countkeys = CountKeys(keys=keys, day=day)
            db.session.add(new_countkeys)
            db.session.commit()
            day += timedelta(days=1)

        


                
                
                




