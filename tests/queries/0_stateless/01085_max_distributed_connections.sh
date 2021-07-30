#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

opts=(
    --max_distributed_connections 9
    --max_threads 1
    --query "SELECT sleepEachRow(1) FROM remote('127.{2..10}', system.one)"
)
# 5 less then 9 seconds (9 streams), but long enough to cover possible load peaks
# "$@" left to pass manual options (like --experimental_use_processors 0) during manual testing
timeout 5s ${CLICKHOUSE_CLIENT} "${opts[@]}" "$@"
