#!/usr/bin/env bash
# Tags: distributed, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Sometimes 1.8 seconds are not enough due to system overload.
# But if it can run in less than five seconds at least sometimes - it is enough for the test.

i=0 retries=100
while [[ $i -lt $retries ]]; do
    query="SELECT sleepEachRow(1) FROM remote('127.{2,3}', system.one) FORMAT Null"
    # 1.8 less then 2 seconds, but long enough to cover possible load peaks
    timeout 1.8s ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_distributed_connections=2&max_threads=1" -d "$query" && break
    ((++i))
done

clickhouse_test_wait_queries 60
