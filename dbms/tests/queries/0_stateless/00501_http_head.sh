#!/usr/bin/env bash

( curl -s --head "${CLICKHOUSE_URL:=http://localhost:8123/}?query=SELECT%201";
  curl -s --head "${CLICKHOUSE_URL:=http://localhost:8123/}?query=select+*+from+system.numbers+limit+1000000" ) | grep -v "Date:"
