#!/bin/bash

set -e -x -a

wget 'https://dl.min.io/server/minio/release/linux-amd64/minio'
chmod +x minio

./minio server --address ":11111" ./data > /dev/null &

wget 'https://dl.min.io/client/mc/release/linux-amd64/mc'
chmod +x ./mc

./mc alias set clickminio http://localhost:11111 clickhouse clickhouse
./mc admin user add clickminio test testtest
./mc admin policy set clickminio readwrite user=test
./mc mb clickminio/test


# Upload data to Minio. By default after unpacking all tests will in
# /usr/share/clickhouse-test/queries

cd /usr/share/clickhouse-test/queries/0_stateless/test_minio

FILES=$(ls .)
for FILE in $FILES; do
    echo $FILE;
    ./mc cp $FILE clickminio/test/$FILE;
done
