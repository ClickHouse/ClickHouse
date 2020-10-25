#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

opts=(
    --max_distributed_connections 2
    --max_threads 1
    --query "SELECT sleepEachRow(1) FROM remote('127.{2,3}', system.one)"
)
# 1.8 less then 2 seconds, but long enough to cover possible load peaks
# "$@" left to pass manual options (like --experimental_use_processors 0) during manual testing
timeout 1.8s ${CLICKHOUSE_CLIENT} "${opts[@]}" "$@"
