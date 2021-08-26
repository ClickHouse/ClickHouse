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

echo -e "a b c\na b c\na b c\n" > a.txt
./mc cp a.txt clickminio/test
