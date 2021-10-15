#!/usr/bin/env bash
# Tags: distributed

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

i=0 retries=300
# Sometimes five seconds are not enough due to system overload.
# But if it can run in less than five seconds at least sometimes - it is enough for the test.
while [[ $i -lt $retries ]]; do
    opts=(
        --max_distributed_connections 20
        --max_threads 1
        --query "SELECT sleepEachRow(1) FROM remote('127.{2..21}', system.one)"
        --format Null
    )
    # 10 less then 20 seconds (20 streams), but long enough to cover possible load peaks
    # "$@" left to pass manual options (like --experimental_use_processors 0) during manual testing

    timeout 10s ${CLICKHOUSE_CLIENT} "${opts[@]}" "$@" && break
    ((++i))
done
