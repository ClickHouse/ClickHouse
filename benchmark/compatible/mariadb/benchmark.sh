#!/bin/bash

# Install

sudo apt-get update
sudo apt-get install mariadb-server
sudo bash -c "echo -e '[mysql]\nlocal-infile=1\n\n[mysqld]\nlocal-infile=1\n' > /etc/mysql/conf.d/local_infile.cnf"
sudo service mariadb restart

# Load the data

wget 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gzip -d hits.tsv.gz

sudo mysql -e "CREATE DATABASE test"
sudo mysql test < create.sql
time sudo mysql test -e "LOAD DATA LOCAL INFILE 'hits.tsv' INTO TABLE hits"

# 2:23:45 elapsed

./run.sh 2>&1 | tee log.txt
