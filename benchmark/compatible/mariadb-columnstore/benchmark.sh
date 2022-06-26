#!/bin/bash

# Install

sudo apt-get update
sudo apt-get install docker.io
sudo docker run -d -p 3306:3306 -e ANALYTICS_ONLY=1 --name mcs_container mariadb/columnstore

export PASSWORD="tsFgm457%3cj"
sudo docker exec mcs_container mariadb -e "GRANT ALL PRIVILEGES ON *.* TO 'ubuntu'@'%' IDENTIFIED BY '${PASSWORD}';"

sudo apt install mariadb-client

mysql --password="${PASSWORD}" --host 127.0.0.1 -e "CREATE DATABASE test"
mysql --password="${PASSWORD}" --host 127.0.0.1 test < create.sql

# Load the data

wget 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gzip -d hits.tsv.gz

time mysql --password="${PASSWORD}" --host 127.0.0.1 test -e "LOAD DATA LOCAL INFILE 'hits.tsv' INTO TABLE hits"
