#!/bin/bash

sudo apt-get update
sudo apt-get install postgresql-common
sudo apt-get install postgresql-14

wget 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gzip -d hits.tsv.gz
chmod 777 ~ hits.tsv

sudo -u postgres psql -t -c 'CREATE DATABASE test'
sudo -u postgres psql test -t < create.sql
sudo -u postgres psql test -t -c '\timing' -c "\\copy hits FROM 'hits.tsv'"

# COPY 99997497
# Time: 2341543.463 ms (39:01.543)

./run.sh | tee log.txt
