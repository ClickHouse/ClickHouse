#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# regression for 'Empty task was returned from async task queue' during query
# cancellation with async_socket_for_remote=1 (that ignores
# max_distributed_connections)
timeout 5s ${CLICKHOUSE_CLIENT} --max_distributed_connections=1 --format Null -q "select * from remote('127.{2..11}', view(select * from numbers(1e9))) group by number format Null"
# timedout
test $? -eq 124
