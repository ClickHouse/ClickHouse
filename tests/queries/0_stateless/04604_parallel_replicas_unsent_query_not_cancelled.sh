#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SELECT ... LIMIT 1` over parallel replicas where the initiator's local replica takes the only
# mark: the coordinator never reaches `Finish: true`, so the callback that cancels unused replicas
# never runs and teardown is the only thing that can release the followers.
#
# `RemoteQueryExecutor::finish` used to mark such an executor `finished` without marking it
# `was_cancelled` when the query had not been sent yet. Since the send paths gate only on
# `was_cancelled`, an already scheduled `RemoteSource::work()` still sent the query afterwards,
# and nothing could release it after that: no `Cancel` packet and no disconnect. The follower then
# stayed blocked in `receivePartitionMergeTreeReadTaskResponse` for the whole `receive_timeout`,
# holding the table's shared lock and stalling a subsequent `DROP TABLE`.
#
# `async_query_sending_for_remote = 0` makes the window deterministic: the query is sent
# synchronously from `work()`, so `finish()` observes an executor that has neither sent its query
# nor created a read context.
#
# `receive_timeout` is propagated to the replica and is the timeout a parked one is stuck on. It is
# lowered here only so that a regression fails in seconds instead of hanging for the default 300,
# and it is kept well above the window this test polls for.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_04604 SYNC;
    CREATE TABLE t_04604 (x String) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_04604 SELECT toString(number) FROM numbers(10);
"

query_id="04604_${CLICKHOUSE_DATABASE}_$RANDOM"

$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    SELECT x FROM t_04604 LIMIT 1 FORMAT Null
    SETTINGS enable_parallel_replicas = 1,
             max_parallel_replicas = 3,
             cluster_for_parallel_replicas = 'parallel_replicas',
             parallel_replicas_for_non_replicated_merge_tree = 1,
             parallel_replicas_local_plan = 1,
             automatic_parallel_replicas_mode = 0,
             async_query_sending_for_remote = 0,
             receive_timeout = 10
"

# Every replica query the initiator started must be gone once the initiator returned. A replica
# left in `system.processes` is one parked on a read task that will never be answered. Poll for a
# couple of seconds so that a merely slow shutdown is not reported as a leak - that is far less
# than `receive_timeout`, so a parked replica is still there when the loop gives up.
for _ in {1..10}; do
    left=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.processes
        WHERE initial_query_id = '$query_id' AND query_id != '$query_id'
    ")
    [[ "$left" == "0" ]] && break
    sleep 0.2
done

if [[ "$left" == "0" ]]; then
    echo "no parked replicas"
else
    echo "FAIL: $left replica(s) still running after the initiator finished"
fi

# The parked replica holds a shared lock on the table, so this is the statement that would block.
$CLICKHOUSE_CLIENT -q "DROP TABLE t_04604 SYNC"
