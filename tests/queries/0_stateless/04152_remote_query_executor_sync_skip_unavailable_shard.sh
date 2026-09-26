#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# One available shard + one unavailable shard, read synchronously
# (async_socket_for_remote=0) so RemoteQueryExecutor::read() runs. With
# skip_unavailable_shards=1 the unavailable shard yields an empty connections
# set and read() must return an empty block for it; the available shard
# returns the single system.one row -> count() = 1.
$CLICKHOUSE_CLIENT --query "
SELECT count() FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP},127.0.0.1:1', system, one)
SETTINGS skip_unavailable_shards = 1,
         async_socket_for_remote = 0,
         async_query_sending_for_remote = 0,
         prefer_localhost_replica = 0,
         use_hedged_requests = 0,
         send_logs_level = 'fatal'
"
