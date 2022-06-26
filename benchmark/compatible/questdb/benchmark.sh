#!/bin/bash

# Install

wget https://github.com/questdb/questdb/releases/download/6.4.1/questdb-6.4.1-rt-linux-amd64.tar.gz
tar xf questdb*.tar.gz
questdb-6.4.1-rt-linux-amd64/bin/questdb.sh start

# Import the data

wget 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz'
gzip -d hits.csv.gz

curl -G --data-urlencode "query=$(cat create.sql)" 'http://localhost:9000/exec?timings=true'
time curl -F data=@hits.csv 'http://localhost:9000/imp?name=hits'

# 27m 47.546s

./run.sh 2>&1 | tee log.txt

du -bcs .questdb/db/hits

cat log.txt | grep -P '"timings"|"error"|^$' | sed -r -e 's/^.*"error".*$|^$/null/; s/^.*"compiler":([0-9]*),"execute":([0-9]*),.*$/\1 \2/' |
  awk '{ if ($1 == "null") { print } else { print ($1 + $2) / 1000000000 } }' |
  awk '{ if (i % 3 == 0) { printf "[" }; printf $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'
