#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import os
import math
import zipfile

# Credentials needed to access the API and make requests.
# NEEDED

# API key
consumerkey = ''
# API secret
consumersecret = ''
# Access token
accesstoken = ''
# Access token secret
accesstokensecret = ''

authorization = OAuthHandler(consumerkey, consumersecret)
authorization.set_access_token(accesstoken, accesstokensecret)

class Listener(StreamListener):
    def __init__(self, api=None, path=None, jsonfilename=None):
        self.api = api
        self.siesta = 0
        self.dodo = 0
        self.beginning = time.time()
        self.end = time.time()
        self.timediff = 0
        self.path = path

    def on_data(self, data):
        self.siesta = 0
        self.dodo = 0

        self.end = time.time()
        self.timediff = self.end - self.beginning

        if self.timediff < 1800.0:
            try:
                #Extraction of the tweet text for further use.
                tweet = data.split(',"text":"')[1].split('","source":"')[0]
                print time.strftime("%Y%m%d_%H%M%S"), tweet
                savefile = open(file_path, 'ab')
                savefile.write(data)
                savefile.close()

            except BaseException, e:
                #Prints the exception message.
                print 'Failed on_data() with this error:', str(e)

            return True
        else:
            filename = file_path + ".zip"
            zf = zipfile.ZipFile(
                filename, 
                mode='w', 
                compression=zipfile.ZIP_DEFLATED
                )
            zf.write(file_path, jsonfilename)
            zf.close()

            return False

    def on_error(self, status_code):
        print 'Error:', str(status_code)

        if status_code == 420:
            siesta = 60 * math.pow(2, self.siesta)
            print time.strftime("%Y%m%d_%H%M%S")
            print "A reconnection attempt will occur in " + \
            str(siesta/60) + " minutes."
            print '''
            *******************************************************************
            From Twitter Streaming API Documentation
            420: Rate Limited
            The client has connected too frequently. For example, an 
            endpoint returns this status if:
            - A client makes too many login attempts in a short period 
              of time.
            - Too many copies of an application attempt to authenticate 
              with the same credentials.
            *******************************************************************
            '''
            # Source: https://dev.twitter.com/docs/streaming-apis/connecting
            time.sleep(siesta)
            self.siesta += 1
        else:
            dodo = 5 * math.pow(2, self.dodo)
            print "A reconnection attempt will occur in " + \
            str(dodo) + " seconds."
            time.sleep(dodo)
            self.dodo += 1
        return True

while True:
    timestr = time.strftime("%Y%m%d_%H%M%S")
    print timestr

    script_dir = os.path.dirname(__file__)
    rel_path = "Tweets_" + timestr + ".json"
    file_path = os.path.join(script_dir, rel_path)
    jsonfilename = "Tweets_" + timestr + ".json"

    twitterStream = Stream(
        authorization, 
        Listener(
            path=file_path,
            jsonfilename=jsonfilename
            ), 
        timeout=60
        )

    try:
        twitterStream.filter(locations=[-74.0231,45.3299,-73.3846,45.7311])
    
    except Exception, e:
        print 'Failed filter() with this error:', str(e)
