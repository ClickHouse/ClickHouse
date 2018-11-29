#!/usr/bin/env bash

for (( i = 0; i < 1000; i++ )); do
    if (( RANDOM % 10 )); then
        clickhouse-client --port=9007 --query="INSERT INTO mt (x) SELECT rand64() AS x FROM system.numbers LIMIT 100000"
    else
        clickhouse-client --port=9007 --query="INSERT INTO mt (x) SELECT rand64() AS x FROM system.numbers LIMIT 300000"
    fi

done
