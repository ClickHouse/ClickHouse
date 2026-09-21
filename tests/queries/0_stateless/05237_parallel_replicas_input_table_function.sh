#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `parallel_replicas_min_number_of_rows_per_replica` makes the planner estimate how many rows the
# query will read in order to pick a replica count. Answering that estimate must not build the IN
# set: `input()` is a one-shot stream from the client and cannot be read a second time.
PR="enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1"
PR="$PR, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost'"
PR="$PR, max_parallel_replicas = 3, parallel_replicas_local_plan = 1"
PR="$PR, parallel_replicas_min_number_of_rows_per_replica = 1"
# The estimate is reached both directly and from the automatic-parallel-replicas probe plan, so pin
# the mode explicitly instead of inheriting it.
PR_DIRECT="$PR, automatic_parallel_replicas_mode = 0"
PR_AUTO="$PR, automatic_parallel_replicas_mode = 2"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS src, dst_http, dst_tcp, dst_auto;
CREATE TABLE src (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 16;
INSERT INTO src SELECT number, number * 2 FROM numbers(20000);
CREATE TABLE dst_http (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE dst_tcp (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE dst_auto (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
"

# Every case prints 1 for a counter that reads 0 unless that case really planned for parallel
# replicas. The row oracles cannot carry that themselves: with
# `parallel_replicas_allow_in_with_subquery = 0` the route is declined for all three `input()`
# queries and they still insert exactly the rows below.
# The query_log row lands after the query returns, so poll for it. Any row but `QueryStart` ends the
# wait, so a query that threw reports 0 straight away instead of polling to the limit first.
query_log_says()
{
    local answer
    for _ in {1..120}
    do
        $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
        answer=$($CLICKHOUSE_CLIENT -q "
            SELECT $2
            FROM system.query_log
            WHERE current_database = currentDatabase() AND query_id = '$1'
              AND type != 'QueryStart' AND is_initial_query")
        [ -n "$answer" ] && break
        sleep 0.5
    done
    echo "$answer"
}

route_taken()
{
    query_log_says "$1" "ProfileEvents['$2'] > 0"
}

# A query with no IN set to begin with, so the estimate still answers it: what follows is a
# restriction on set-bearing queries only.
# `sum` and not `count`: a trivial count disables parallel replicas by itself.
# A fresh query id per execution: this test may be retried against the same database, and an id from
# an earlier attempt would answer for the current one.
QUERY_ID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -q "SELECT sum(v) FROM src WHERE k % 3 = 1 SETTINGS $PR_DIRECT FORMAT Null"
route_taken "$QUERY_ID" ParallelReplicasUsedCount

# `GLOBAL IN` and not `IN` in the two cases that read from a replica: a plain `IN (subquery)` is
# re-executed on every follower, and a follower has no client input stream, so it cannot read
# `input()` at all. `GLOBAL IN` evaluates the subquery on the initiator and ships the result as a
# temporary table, which is what lets a multi-replica route serve this query.

# Over HTTP the client data reaches `input()` as an already built pipe.
QUERY_ID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
QUERY="INSERT INTO dst_http SELECT k, v FROM src WHERE k GLOBAL IN (SELECT n FROM input('n UInt64')) SETTINGS $PR_DIRECT FORMAT RowBinary"
{ echo "$QUERY"; $CLICKHOUSE_CLIENT -q "SELECT toUInt64(arrayJoin([7, 19, 12345])) FORMAT RowBinary"; } \
    | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=$QUERY_ID" --data-binary @-
$CLICKHOUSE_CLIENT -q "SELECT k, v FROM dst_http ORDER BY ALL"
route_taken "$QUERY_ID" ParallelReplicasUsedCount
# The executed read must still prune by the set it did build: 3 of the 1250 granules here, against
# every one of them for a read whose analysis has no set. Answering the estimate with
# `KeyCondition`'s `require_ready_sets` would also stop reading `input()` twice, but it memoizes a
# set-less analysis that the executed read then inherits, which this line rejects.
query_log_says "$QUERY_ID" "ProfileEvents['SelectedMarks'] < 50"

# Over the native protocol it reaches `input()` through the input callback instead.
QUERY_ID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT -q "SELECT toUInt64(arrayJoin([7, 19, 12345])) FORMAT RowBinary" \
    | $CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -q "INSERT INTO dst_tcp SELECT k, v FROM src WHERE k GLOBAL IN (SELECT n FROM input('n UInt64')) SETTINGS $PR_DIRECT FORMAT RowBinary"
$CLICKHOUSE_CLIENT -q "SELECT k, v FROM dst_tcp ORDER BY ALL"
route_taken "$QUERY_ID" ParallelReplicasUsedCount

# Statistics-only automatic mode plans the same estimate inside its probe plan. It deliberately
# leaves the read on the initiator, so `ParallelReplicasUsedCount` is 0 for it and no follower is
# contacted at all, which is why a plain `IN` is served here; the statistics collector is installed
# only by considerEnablingParallelReplicas, so its counter stands for that probe plan having been
# built.
QUERY_ID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT -q "SELECT toUInt64(arrayJoin([7, 19, 12345])) FORMAT RowBinary" \
    | $CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -q "INSERT INTO dst_auto SELECT k, v FROM src WHERE k IN (SELECT n FROM input('n UInt64')) SETTINGS $PR_AUTO FORMAT RowBinary"
$CLICKHOUSE_CLIENT -q "SELECT k, v FROM dst_auto ORDER BY ALL"
route_taken "$QUERY_ID" RuntimeDataflowStatisticsInputBytes
