#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="${CLICKHOUSE_DATABASE}_$$"
benchmark_args=(
    # A loaded runner can take longer than the default 10 s handshake_timeout_ms
    # to send Hello; the benchmark then exits without running a query and
    # query_log has 0 rows instead of 3.
    --connect_timeout 60
    --handshake_timeout_ms 60000
    --iterations 1
    --log_queries 1
    --query_id "$query_id"
    --log_queries_min_type QUERY_FINISH
)
$CLICKHOUSE_BENCHMARK "${benchmark_args[@]}" --query "SELECT * FROM remote('127.2', 'system.one')" >& /dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
# Number of queries:
# - DESC TABLE system.one
# - query on initiator
# - query on shard
# Total: 3
#
# -- NOTE: this test cannot use 'current_database = $CLICKHOUSE_DATABASE',
# -- because it does not propagated via remote queries,
# -- but it uses query_id, and this is enough.
$CLICKHOUSE_CLIENT --param_query_id="$query_id" -q "SELECT count() FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND initial_query_id = {query_id:String}"
