#!/bin/bash
# shellcheck disable=SC2024

set -e -x -a -u

ls -lha

cd /hadoop-3.3.1

export JAVA_HOME=/usr
mkdir -p target/test/data

bin/mapred minicluster -format -nomr -nnport 12222 &

while ! nc -z localhost 12222; do
  sleep 1
done

lsof -i :12222
