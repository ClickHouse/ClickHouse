#!/bin/bash

sudo apt-get update
sudo apt-get install docker.io
sudo apt-get install postgresql-client

sudo docker run -d --name citus -p 5432:5432 -e POSTGRES_PASSWORD=mypass citusdata/citus:11.0

wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gzip -d hits.tsv.gz

echo "*:*:*:*:mypass" > .pgpass
chmod 400 .pgpass

psql -U postgres -h localhost --no-password -t -c 'CREATE DATABASE test'
psql -U postgres -h localhost --no-password test -t < create.sql
psql -U postgres -h localhost --no-password test -t -c '\timing' -c "\\copy hits FROM 'hits.tsv'"

# COPY 99997497
# Time: 2341543.463 ms (39:01.543)

./run.sh 2>&1 | tee log.txt

#sudo du -bcs /var/lib/postgresql/14/main/

cat log.txt | grep -oP 'Time: \d+\.\d+ ms' | sed -r -e 's/Time: ([0-9]+\.[0-9]+) ms/\1/' |
    awk '{ if (i % 3 == 0) { printf "[" }; printf $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'
