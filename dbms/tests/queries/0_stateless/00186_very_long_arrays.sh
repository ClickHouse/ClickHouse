#!/usr/bin/env bash

(echo 'SELECT number FROM system.numbers WHERE transform(number, ['; seq 1 100000 | tr '\n' ','; echo '0],['; seq 1 100000 | tr '\n' ','; echo '0]) = 10000000 LIMIT 1';) | clickhouse-client --max_query_size=100000000
