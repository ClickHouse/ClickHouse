#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="${CLICKHOUSE_DATABASE}_$$"
benchmark_args=(
    --iterations 1
    --log_queries 1
    --query_id "$query_id"
    --log_queries_min_type QUERY_FINISH
)
$CLICKHOUSE_BENCHMARK "${benchmark_args[@]}" --query "SELECT * FROM remote('127.2', 'system.one')" >& /dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
# Number of queries:
# - DESC TABLE system.one
# - query on initiator
# - query on shard
# Total: 3
#
# -- NOTE: this test cannot use 'current_database = $CLICKHOUSE_DATABASE',
# -- because it does not propagated via remote queries,
# -- but it uses query_id, and this is enough.
$CLICKHOUSE_CLIENT --param_query_id="$query_id" -q "SELECT count() FROM system.query_log WHERE event_date >= yesterday() AND initial_query_id = {query_id:String}"
