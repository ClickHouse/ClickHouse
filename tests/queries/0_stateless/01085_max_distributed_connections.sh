#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Sometimes five seconds are not enough due to system overload.
# But if it can run in less than five seconds at least sometimes - it is enough for the test.
while true
do
    opts=(
        --max_distributed_connections 9
        --max_threads 1
        --query "SELECT sleepEachRow(1) FROM remote('127.{2..10}', system.one)"
        --format Null
    )
    # 5 less then 9 seconds (9 streams), but long enough to cover possible load peaks
    # "$@" left to pass manual options (like --experimental_use_processors 0) during manual testing

    timeout 5s ${CLICKHOUSE_CLIENT} "${opts[@]}" "$@" && break
done
