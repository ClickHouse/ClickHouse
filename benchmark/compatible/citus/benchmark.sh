#!/bin/bash

sudo apt-get update
sudo apt-get install -y docker.io
sudo apt-get install -y postgresql-client

sudo docker run -d --name citus -p 5432:5432 -e POSTGRES_PASSWORD=mypass citusdata/citus:11.0

wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gzip -d hits.tsv.gz

echo "*:*:*:*:mypass" > .pgpass
chmod 400 .pgpass

psql -U postgres -h localhost -d postgres --no-password -t -c 'CREATE DATABASE test'
psql -U postgres -h localhost -d postgres --no-password test -t < create.sql
psql -U postgres -h localhost -d postgres --no-password test -t -c '\timing' -c "\\copy hits FROM 'hits.tsv'"

# COPY 99997497
# Time: 1579203.482 ms (26:19.203)

./run.sh 2>&1 | tee log.txt

sudo docker exec -it citus du -bcs /var/lib/postgresql/data

cat log.txt | grep -oP 'Time: \d+\.\d+ ms' | sed -r -e 's/Time: ([0-9]+\.[0-9]+) ms/\1/' |
    awk '{ if (i % 3 == 0) { printf "[" }; printf $1 / 1000; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'
