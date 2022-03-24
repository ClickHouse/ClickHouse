#!/bin/bash

set -e -x -a -u

ls -lha

cd hadoop-3.2.2

export JAVA_HOME=/usr

bin/mapred minicluster -format -nomr -nnport 12222 &

while ! curl -v --silent http://localhost:12222 2>&1 | grep 404
do
  echo "Trying to connect to local hdfs"
  sleep 1
done

lsof -i :12222

sleep 5
