#!/usr/bin/env bash
# Tags: long, replica, zookeeper, no-shared-merge-tree

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

KILL_OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_kill.out"
DROP_OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_drop.out"
trap 'rm -f "$KILL_OUT" "$DROP_OUT"' EXIT

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE r1 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04740/t', 'r1') ORDER BY k;
    CREATE TABLE r2 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04740/t', 'r2') ORDER BY k;
    INSERT INTO r1 VALUES (1);
    SYSTEM SYNC REPLICA r2;
"

# Blocks pullLogsToQueue for r2 (throws ABORTED) while r2's is_active node survives, so an
# alter_sync = 2 wait for r2 can never be satisfied and cannot escape through the inactive-replica
# path. replication_wait_for_inactive_replica_timeout = -1 pins that wait to unlimited, so a green
# result cannot come from the pre-existing timeout.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP PULLING REPLICATION LOG r2"

TRUNCATE_UNLIMITED="TRUNCATE TABLE r1 SETTINGS alter_sync = 2, replication_wait_for_inactive_replica_timeout = -1"

# Waits until exactly one TRUNCATE of this test's database is waiting in the process list.
wait_for_truncate()
{
    for _ in {1..600}; do
        if [ "$($CLICKHOUSE_CLIENT -q "
                    SELECT count() FROM system.processes
                    WHERE current_database = currentDatabase() AND query LIKE 'TRUNCATE%'")" = "1" ]; then
            return 0
        fi
        sleep 0.5
    done
    echo 'TRUNCATE never appeared in system.processes'
}

# 1. max_execution_time must terminate the wait.
$CLICKHOUSE_CLIENT -q "
    TRUNCATE TABLE r1 SETTINGS alter_sync = 2, max_execution_time = 5,
        replication_wait_for_inactive_replica_timeout = -1
" 2>&1 | grep -om1 'Code: 159.*Timeout exceeded: elapsed [0-9.]* ms, maximum: 5000 ms' \
       | sed 's/DB::Exception: //g; s/elapsed [0-9.]* ms, //'

# 2. KILL QUERY must terminate the wait.
$CLICKHOUSE_CLIENT -q "$TRUNCATE_UNLIMITED" > "$KILL_OUT" 2>&1 &
wait_for_truncate
query_id=$($CLICKHOUSE_CLIENT -q "
    SELECT query_id FROM system.processes
    WHERE current_database = currentDatabase() AND query LIKE 'TRUNCATE%' LIMIT 1")
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC" > /dev/null
wait
grep -om1 'Code: 394.*Query was cancelled' "$KILL_OUT" | sed 's/DB::Exception: //g; s/Received from [^ ]* //'

# 3. Dropping the database calls flushAndPrepareForShutdown on both tables, which stops r2 from
# ever processing the entry. The waiting TRUNCATE must give up so the DROP is not deadlocked behind
# it. No cancellation is involved here, so this covers the shutdown escape on its own.
$CLICKHOUSE_CLIENT -q "$TRUNCATE_UNLIMITED" > "$DROP_OUT" 2>&1 &
wait_for_truncate
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${CLICKHOUSE_DATABASE} SYNC"
echo 'database dropped'
wait
grep -om1 'Code: 341.*Timeout exceeded while waiting for replicas r2 to process entry log-[0-9]*' "$DROP_OUT" \
    | sed 's/DB::Exception: //g; s/Received from [^ ]* //; s/log-[0-9]*/log-N/'
