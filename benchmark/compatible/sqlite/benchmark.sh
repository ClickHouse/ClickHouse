#!/bin/bash

sudo apt-get update
sudo apt-get install -y sqlite3

sqlite3 mydb < create.sql

wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz'
gzip -d hits.csv.gz

time sqlite3 mydb '.import --csv hits.csv hits'
wc -c mydb

./run.sh 2>&1 | tee log.txt
