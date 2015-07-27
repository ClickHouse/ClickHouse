#!/bin/bash

curl http://localhost:8123/?extremes=1 -d @- <<< "DROP TABLE IF EXISTS test.test"
curl http://localhost:8123/?extremes=1 -d @- <<< "CREATE TABLE test.test (x UInt8) ENGINE = Log"
curl http://localhost:8123/?extremes=1 -d @- <<< "INSERT INTO test.test SELECT 1 AS x"
curl http://localhost:8123/?extremes=1 -d @- <<< "DROP TABLE test.test"
