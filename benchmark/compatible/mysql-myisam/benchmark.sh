#!/bin/bash

# Install

sudo apt-get update
sudo apt-get install -y mysql-server-8.0
sudo bash -c "echo -e '[mysql]\nlocal-infile=1\n\n[mysqld]\nlocal-infile=1\n' > /etc/mysql/conf.d/local_infile.cnf"
sudo service mysql restart

# Load the data

wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gzip -d hits.tsv.gz

sudo mysql -e "CREATE DATABASE test"
sudo mysql test < create.sql
time sudo mysql test -e "LOAD DATA LOCAL INFILE 'hits.tsv' INTO TABLE hits"

#

./run.sh 2>&1 | tee log.txt

sudo du -bcs /var/lib/mysql

cat log.txt |
  grep -P 'rows? in set|^ERROR' |
  sed -r -e 's/^ERROR.*$/null/; s/^.*?\((([0-9.]+) min )?([0-9.]+) sec\).*?$/\2 \3/' |
  awk '{ if ($2) { print $1 * 60 + $2 } else { print $1 } }' |
  awk '{ if (i % 3 == 0) { printf "[" }; printf $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'
