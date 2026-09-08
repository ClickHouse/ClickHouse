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
    # 1.8 less then 2 seconds, but long enough to cover possible load peaks.
    # prefer_localhost_replica=0 forces real remote connections so the test exercises
    # max_distributed_connections (parallel remote reads); otherwise on the macOS runner the
    # lo0-aliased 127.0.0.2/3 are local and get executed serially under max_threads=1. See the
    # note in 01085_max_distributed_connections.sh.
    timeout 1.8s ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_distributed_connections=2&max_threads=1&prefer_localhost_replica=0" -d "$query" && break
    ((++i))
done
