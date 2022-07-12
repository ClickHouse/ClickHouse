#!/bin/bash

bash -c "$(curl -L https://try.crate.io/)" > crate.log 2>&1 &

sudo apt-get update
sudo apt-get install -y postgresql-client

psql -U crate -h localhost --no-password -t -c 'SELECT 1'

wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gzip -d hits.tsv.gz

psql -U crate -h localhost --no-password -t < create.sql

psql -U crate -h localhost --no-password -t -c '\timing' -c "
  COPY hits
  FROM 'file://$(pwd)/hits.tsv'
  WITH
  (
    "delimiter"=e'\t',
    "format"='csv',
    "header"=false,
    "empty_string_as_null"=false
  )
  RETURN SUMMARY;"

# One record did not load:
# 99997496
# {"Missing closing quote for value\n at [Source: UNKNOWN; line: 1, column: 1069]":{"count":1,"line_numbers":[93557187]}}
# Time: 10687056.069 ms (02:58:07.056)

./run.sh 2>&1 | tee log.txt

# For some queries it gives "Data too large".

du -bcs crate-*

cat log.txt | grep -oP 'Time: \d+\.\d+ ms|ERROR' | sed -r -e 's/Time: ([0-9]+\.[0-9]+) ms/\1/' |
  awk '{ if ($1 == "ERROR") { skip = 1 } else { if (i % 3 == 0) { printf "[" }; printf skip ? "null" : ($1 / 1000); if (i % 3 != 2) { printf "," } else { print "]," }; ++i; skip = 0; } }'
