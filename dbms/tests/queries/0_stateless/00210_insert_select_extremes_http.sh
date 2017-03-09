#!/usr/bin/env bash

curl -sS http://localhost:8123/?extremes=1 -d @- <<< "DROP TABLE IF EXISTS test.test"
curl -sS http://localhost:8123/?extremes=1 -d @- <<< "CREATE TABLE test.test (x UInt8) ENGINE = Log"
curl -sS http://localhost:8123/?extremes=1 -d @- <<< "INSERT INTO test.test SELECT 1 AS x"
curl -sS http://localhost:8123/?extremes=1 -d @- <<< "DROP TABLE test.test"
