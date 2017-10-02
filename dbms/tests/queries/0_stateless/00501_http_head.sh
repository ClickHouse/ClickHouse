#!/usr/bin/env bash

( curl -s --head "${CLICKHOUSE_URL:=http://localhost:8123/}?query=SELECT%201";
  curl -s --head "${CLICKHOUSE_URL:=http://localhost:8123/}?query=select+*+from+system.numbers+limit+1000000" ) | grep -v "Date:"

if [[ `curl -sS -X POST -I "http://localhost:8123?query=SELECT+1" | grep -c '411 Length Required'` -ne 1 ]]; then
    echo FAIL
fi
