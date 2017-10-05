#!/usr/bin/env bash

CLICKHOUSE_URL=${CLICKHOUSE_URL:=http://localhost:8123/}

( curl -s --head "${CLICKHOUSE_URL}?query=SELECT%201";
  curl -s --head "${CLICKHOUSE_URL}?query=select+*+from+system.numbers+limit+1000000" ) | grep -v "Date:"

if [[ `curl -sS -X POST -I "${CLICKHOUSE_URL}?query=SELECT+1" | grep -c '411 Length Required'` -ne 1 ]]; then
    echo FAIL
fi
