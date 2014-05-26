#!/usr/bin/env python
#
# This script downloads data from NSE and sumps to local db : you can
# redistribute it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, version 2.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
# Copyright sivamgr@gmail.com

import datetime
import sqlite3
import os
import os.path
import csv
import time
import pycurl
import zipfile
import sys, getopt

def main(argv):
	dbfile=""
	try:
		opts, args = getopt.getopt(argv,"ho:",["ofile="])
	except getopt.GetoptError:
		print 'test.py -i <inputfile> -o <outputfile>'
		sys.exit(2)

	for opt, arg in opts:
		if opt == '-h':
			print 'pull_bhav.py -o <sqlite_dbfile>'
			sys.exit()
		elif opt in ("-o", "--ofile"):
			dbfile = arg
		
	mydate =  datetime.date.today()
	oneday = datetime.timedelta(days=1)
	months = ["","JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
	str_num = ["00","01","02","03","04","05","06","07","08","09","10","11","12"]

	conn = sqlite3.connect(dbfile)
	curr = conn.cursor()

	try:
		curr.execute('''CREATE TABLE eq_daily(symbol varchar(16), open REAL, high REAL, low REAL, close REAL, prevclose REAL, traded_qty INTEGER, traded_val REAL, dt DATE, num_trades INTEGER, PRIMARY KEY (symbol,dt))''')
	except:
		print "valid database"

	curr.execute('''select max(dt) as "max_dt [timestamp]" from eq_daily''')
	data = curr.fetchall()
	if len(data) == 0:
		diff_days = 3660
	elif data[0][0]==None:
		diff_days = 3660
	else:
		#last_date = time.strptime(str(data[0][0]),"%Y-%m-%d")
		last_date = datetime.datetime.strptime(str(data[0][0]),"%d-%b-%Y").date()
		#last_date = dateutil.parser.parse(str(data[0][0]))
		diff = mydate - last_date
		diff_days = diff.days
	if diff_days == 0:
		sys.exit(0)
	try:
		for x in range(1, diff_days):
			mydate = mydate - oneday
			if(mydate.day < 10):
				strday = str_num[mydate.day]
			else:
				strday = str(mydate.day)

			myurl = "http://www.nseindia.com/content/historical/EQUITIES/" + str(mydate.year) + "/" + months[mydate.month] + "/cm" + strday + months[mydate.month] + str(mydate.year) + "bhav.csv.zip"
			mycsv = "/tmp/cm" + strday + months[mydate.month] + str(mydate.year) + "bhav.csv"
			print "Downloading ", myurl
			fp = open("/tmp/pull_bhav.zip","wb")
			curl = pycurl.Curl()
			curl.setopt(pycurl.URL, myurl)
			curl.setopt(pycurl.USERAGENT, "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:29.0) Gecko/20100101 Firefox/29.0")
			curl.setopt(pycurl.WRITEDATA, fp)
			curl.perform()
			curl.close()
			fp.close()
			sz = os.path.getsize("/tmp/pull_bhav.zip")
			if(sz < 1000):
				continue

			with zipfile.ZipFile("/tmp/pull_bhav.zip") as zf:
				print "Extracting zip file... "
				zf.extractall("/tmp/")
				print "Processing csv file,",mycsv
				csvData = csv.reader(open(mycsv, "rb"))
				csvData.next()
				for row in csvData:
					curr.execute('INSERT OR REPLACE INTO eq_daily VALUES (?,?,?,?,?,?,?,?,?,?)', (row[0],row[2],row[3],row[4],row[5],row[7],row[8],row[9],row[10],row[11]))
				os.remove(mycsv)
				conn.commit()
			os.remove("/tmp/pull_bhav.zip")
	except lite.Error, e:
	    print "Error %s:" % e.args[0]
	    if conn:
		conn.close()
		sys.exit(1)
	finally:
	    if conn:
		conn.close()

if __name__ == "__main__":
   main(sys.argv[1:])	
