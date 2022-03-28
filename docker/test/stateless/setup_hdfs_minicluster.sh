#!/bin/bash

set -e -x -a -u

ls -lha

cd hadoop-3.2.2

export JAVA_HOME=/usr

bin/mapred minicluster -format -nomr -nnport 12222 &


while ! nc -z localhost 12222; do   
  sleep 1
done

lsof -i :12222

sleep 5
