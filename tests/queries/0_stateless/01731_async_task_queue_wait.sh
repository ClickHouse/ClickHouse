#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# regression for 'Empty task was returned from async task queue' during query
# cancellation with async_socket_for_remote=1 (that ignores
# max_distributed_connections)
timeout --signal=SIGINT 1 ${CLICKHOUSE_CLIENT} --max_distributed_connections=1 --max_block_size=2  --interactive_delay=900000 -q "select number + sleep(0.3) as x from remote('127.{2,3}', system.numbers) settings max_block_size = 2" 2>&1 | grep "Empty task was returned from async task queue" || true
