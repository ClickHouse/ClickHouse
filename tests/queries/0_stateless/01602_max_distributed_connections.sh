#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

common_opts=(
    "--format=Null"

    # This is the regression test for the case when max_threads was used over
    # max_distributed_connections (with prefer_localhost_replica=1)
    "--max_threads=1"
    "--max_distributed_connections=3"
    # NOTE: that max_distributed_connections limiting does not work with
    # async_socket_for_remote, so disabling it
    "--async_socket_for_remote=0"
    "--use_hedged_requests=0"
)

# NOTE: the test use higher timeout to avoid flakiness.
timeout 9s ${CLICKHOUSE_CLIENT} "$@" "${common_opts[@]}" -q "select sleep(3) from remote('127.{1,2,3,4,5}', system.one)" --prefer_localhost_replica=0
timeout 9s ${CLICKHOUSE_CLIENT} "$@" "${common_opts[@]}" -q "select sleep(3) from remote('127.{1,2,3,4,5}', system.one)" --prefer_localhost_replica=1
