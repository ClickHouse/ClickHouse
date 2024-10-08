#!/bin/bash
# shellcheck disable=SC2024

set -e -x -a -u

ls -lha

cd /hadoop-3.3.1

export JAVA_HOME=/usr
mkdir -p target/test/data
chown clickhouse ./target/test/data
sudo -E -u clickhouse bin/mapred minicluster -format -nomr -nnport 12222 >> /test_output/hdfs_minicluster.log 2>&1 &

while ! nc -z localhost 12222; do
  sleep 1
done

lsof -i :12222

sleep 5
