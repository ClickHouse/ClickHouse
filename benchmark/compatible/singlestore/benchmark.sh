#!/bin/bash

# Install

sudo apt-get update
sudo apt-get install docker.io

export LICENSE_KEY="BDA4OGMxMGFlNDRkYTQ0MmU4N2NkYjk4Y2MwYTUxMTQ5AAAAAAAAAAAEAAAAAAAAACgwNAIYTJwt51SEitrak4T9P7TyYzWzIRstlokzAhgy+cgwXnsXTU9gzedJ/ztTg1TPdc4jrlQAAA=="
export ROOT_PASSWORD="H@I}xfqKsw}[wfG,oLpH"

sudo docker run -i --init \
    --name memsql-ciab \
    -e LICENSE_KEY="${LICENSE_KEY}" \
    -e ROOT_PASSWORD="${ROOT_PASSWORD}" \
    -p 3306:3306 -p 8080:8080 \
    memsql/cluster-in-a-box

sudo docker start memsql-ciab

sudo docker exec -it memsql-ciab memsql -p"${ROOT_PASSWORD}"

# Load the data

wget 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gzip -d hits.tsv.gz
sudo docker cp hits.tsv memsql-ciab:/

sudo docker exec -it memsql-ciab memsql -p"${ROOT_PASSWORD}" -e "CREATE DATABASE test"
sudo docker exec memsql-ciab memsql -p"${ROOT_PASSWORD}" --database=test -e "USE test; $(cat create.sql)"
time sudo docker exec -it memsql-ciab memsql -vvv -p"${ROOT_PASSWORD}" --database=test -e "LOAD DATA INFILE '/hits.tsv' INTO TABLE test.hits"

# Query OK, 99997497 rows affected (11 min 30.11 sec)

./run.sh 2>&1 | tee log.txt

cat log.txt |
  grep -P 'rows? in set|^ERROR' result.txt |
  sed -r -e 's/^ERROR.*$/null/; s/^.*?\((([0-9.]+) min )?([0-9.]+) sec\).*?$/\2 \3/' |
  awk '{ if ($2) { print $1 * 60 + $2 } else { print $1 } }' |
  awk '{ if (i % 3 == 0) { printf "[" }; printf $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'
