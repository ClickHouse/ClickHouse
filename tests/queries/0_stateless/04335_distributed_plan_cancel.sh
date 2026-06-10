#!/usr/bin/env bash
# Tags: no-fasttest, no-old-analyzer
# no-old-analyzer: make_distributed_plan requires the analyzer.
# Checks that cancelling a distributed-plan query terminates it promptly: cancellation must wake
# worker tasks blocked on in-memory exchanges and interrupt stage-dependency waits.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="distributed_plan_cancel_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_dp_cancel (x UInt64) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_dp_cancel SELECT number FROM numbers(10000000)"

# sleepEachRow is capped at 3 seconds per block, so the per-row delay must keep a full 8192-row
# granule block under the cap; 0.0001s gives ~0.8s per block and a ~1000s natural query duration.
$CLICKHOUSE_CLIENT --query-id="$query_id" -q "
    SELECT x, count() FROM t_dp_cancel WHERE NOT sleepEachRow(0.0001) GROUP BY x FORMAT Null
    SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
        distributed_plan_max_rows_to_broadcast = 0, max_rows_to_group_by = 0, max_execution_time = 300
" 2>/dev/null &
query_pid=$!

for _ in {1..300}; do
    started=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id'")
    [ "$started" -ge 1 ] && break
    sleep 0.1
done

# SYNC waits for the query to actually terminate; bound it so a cancellation hang fails the test
# instead of hanging the runner.
timeout 60 $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null" || echo "KILL timed out"

wait "$query_pid" 2>/dev/null || true

lingering=1
for _ in {1..300}; do
    lingering=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id'")
    [ "$lingering" -eq 0 ] && break
    sleep 0.1
done
echo "lingering queries: $lingering"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_dp_cancel"
