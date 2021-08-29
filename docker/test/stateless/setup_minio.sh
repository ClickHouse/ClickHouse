#!/bin/bash

set -e -x -a

ls -lha

mkdir -p ./data
./minio server --address ":11111" ./data &

while ! curl http://localhost:11111
do
  echo "Trying to connect to minio"
  sleep 1
done

lsof -i :11111

sleep 5

./mc alias set clickminio http://localhost:11111 clickhouse clickhouse
./mc admin user add clickminio test testtest
./mc admin policy set clickminio readwrite user=test
./mc mb clickminio/test


# Upload data to Minio. By default after unpacking all tests will in
# /usr/share/clickhouse-test/queries

cd /usr/share/clickhouse-test/queries/0_stateless/data_minio

FILES=$(ls .)
for FILE in $FILES; do
    echo $FILE;
    /mc cp $FILE clickminio/test/$FILE;
done
