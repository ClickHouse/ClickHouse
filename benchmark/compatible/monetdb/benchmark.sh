#!/bin/bash

# Install

echo "deb https://dev.monetdb.org/downloads/deb/ $(lsb_release -cs) monetdb" | sudo tee /etc/apt/sources.list.d/monetdb.list

sudo wget --output-document=/etc/apt/trusted.gpg.d/monetdb.gpg https://www.monetdb.org/downloads/MonetDB-GPG-KEY.gpg
sudo apt-get update
sudo apt-get install monetdb5-sql monetdb-client

sudo monetdbd create /var/lib/monetdb
sudo monetdbd start /var/lib/monetdb

sudo monetdb create test
sudo monetdb release test

.monetdb

# Load the data

wget 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gzip -d hits.tsv.gz

sudo mysql -e "CREATE DATABASE test"
sudo mysql test < create.sql
sudo time mysql test -e "LOAD DATA LOCAL INFILE 'hits.tsv' INTO TABLE hits"
