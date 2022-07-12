#!/bin/bash

# Install

sudo apt-get update
sudo apt-get install -y mariadb-server
sudo bash -c "echo -e '[mysql]\nlocal-infile=1\n\n[mysqld]\nlocal-infile=1\n' > /etc/mysql/conf.d/local_infile.cnf"
sudo service mariadb restart

# Load the data

wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gzip -d hits.tsv.gz

sudo mariadb -e "CREATE DATABASE test"
sudo mariadb test < create.sql
time sudo mariadb test -e "LOAD DATA LOCAL INFILE 'hits.tsv' INTO TABLE hits"

# 2:23:45 elapsed

./run.sh 2>&1 | tee log.txt

sudo du -bcs /var/lib/mysql

cat log.txt |
  grep -P 'rows? in set|Empty set|^ERROR' |
  sed -r -e 's/^ERROR.*$/null/; s/^.*?\((([0-9.]+) days? )?(([0-9.]+) hours? )?(([0-9.]+) min )?([0-9.]+) sec\).*?$/\2 \4 \6 \7/' |
  awk '{ if ($4) { print $1 * 86400 + $2 * 3600 + $3 * 60 + $4 } else if ($3) { print $1 * 3600 + $2 * 60 + $3 } else if ($2) { print $1 * 60 + $2 } else { print $1 } }' |
  awk '{ if (i % 3 == 0) { printf "[" }; printf $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'
