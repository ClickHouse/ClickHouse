#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

common_opts=(
    "--format=Null"

    "--max_threads=1"
    "--max_distributed_connections=3"
)

# NOTE: the test use higher timeout to avoid flakiness.
timeout 9s ${CLICKHOUSE_CLIENT} "$@" "${common_opts[@]}" -q "select sleep(3) from remote('127.{1,2,3,4,5}', system.one)" --prefer_localhost_replica=0
timeout 9s ${CLICKHOUSE_CLIENT} "$@" "${common_opts[@]}" -q "select sleep(3) from remote('127.{1,2,3,4,5}', system.one)" --prefer_localhost_replica=1
