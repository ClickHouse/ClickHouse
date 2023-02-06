#!/bin/bash

# in order for this script to work, docker must be installed
MYSQL_DOCKER_VER=latest

# trying to conect prematurely will fail, 10s should be enough wait time
MYSQL_DB_START_WAIT=10

echo "running poco-test-mysql docker container"
docker run -p 3306:3306 --name poco-test-mysql -e MYSQL_ROOT_PASSWORD=poco -e MYSQL_DATABASE=pocotestdb -d mysql:$MYSQL_DOCKER_VER > /dev/null

echo "poco-test-mysql container up and running, sleeping $MYSQL_DB_START_WAIT seconds waiting for db to start ..."
sleep $MYSQL_DB_START_WAIT

echo "running tests ..."
./bin/Linux/x86_64/testrunner -all

echo "stopping poco-test-mysql docker container"
docker stop poco-test-mysql > /dev/null

echo "removing poco-test-mysql docker container"
docker rm poco-test-mysql > /dev/null

