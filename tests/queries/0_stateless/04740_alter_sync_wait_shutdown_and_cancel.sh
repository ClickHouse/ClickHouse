#!/usr/bin/env bash
# Tags: long, replica, zookeeper, no-shared-merge-tree, no-replicated-database
# no-replicated-database: creates two explicit replicas (r1, r2) sharing one ZooKeeper path,
#                         and the replica name is rewritten under a Replicated database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

KILL_OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_kill.out"
QUEUE_OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_queue.out"
DROP_OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_drop.out"
trap 'rm -f "$KILL_OUT" "$QUEUE_OUT" "$DROP_OUT"' EXIT

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

# 3. Same, but for the last of the three wait stages. Arms 1 and 2 block pullLogsToQueue, so they
# only ever reach the first stage. Letting r2 pull the entry into its queue and blocking execution
# instead sends the wait into waitForDisappear on the queue node.
$CLICKHOUSE_CLIENT -q "SYSTEM START PULLING REPLICATION LOG r2; SYSTEM STOP REPLICATION QUEUES r2"
# The log maximum has to be sampled before the statement starts: its process list row appears
# before the interpreter creates the log node, so a value read after wait_for_truncate can still
# be the previous maximum.
before=$($CLICKHOUSE_CLIENT -q "
    SELECT log_max_index FROM system.replicas
    WHERE database = currentDatabase() AND table = 'r2'")
$CLICKHOUSE_CLIENT -q "$TRUNCATE_UNLIMITED" > "$QUEUE_OUT" 2>&1 &
wait_for_truncate
# Reaching the third stage requires this TRUNCATE's own entry to be in r2's queue. The advance of
# the log maximum past the pre-statement value is what proves that entry exists, and the observed
# value names it. Comparing log_pointer against a freshly read log_max_index instead would prove
# nothing: log_pointer is the maximum copied entry plus one and may point at an entry that does not
# exist yet, so the arm could degenerate into a copy of arm 2.
entry_index=
for _ in {1..600}; do
    current=$($CLICKHOUSE_CLIENT -q "
        SELECT log_max_index FROM system.replicas
        WHERE database = currentDatabase() AND table = 'r2'")
    if [ -n "$current" ] && [ "$current" -gt "$before" ]; then
        entry_index=$current
        break
    fi
    sleep 0.5
done
if [ -z "$entry_index" ]; then
    echo 'log entry for the TRUNCATE never appeared'
else
    # log_pointer is the maximum copied entry plus one, so this means entry_index was copied.
    queued=
    for _ in {1..600}; do
        if [ "$($CLICKHOUSE_CLIENT -q "
                    SELECT log_pointer > $entry_index FROM system.replicas
                    WHERE database = currentDatabase() AND table = 'r2'")" = "1" ]; then
            queued=1
            echo 'entry queued on r2'
            break
        fi
        sleep 0.5
    done
    [ -n "$queued" ] || echo "r2 never copied log entry $entry_index"
fi
query_id=$($CLICKHOUSE_CLIENT -q "
    SELECT query_id FROM system.processes
    WHERE current_database = currentDatabase() AND query LIKE 'TRUNCATE%' LIMIT 1")
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC" > /dev/null
wait
grep -om1 'Code: 394.*Query was cancelled' "$QUEUE_OUT" | sed 's/DB::Exception: //g; s/Received from [^ ]* //'

# 4. Dropping the database calls flushAndPrepareForShutdown on both tables, which stops r2 from
# ever processing the entry. The waiting TRUNCATE must give up so the DROP is not deadlocked behind
# it. No cancellation is involved here, so this covers the shutdown escape on its own.
$CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATION QUEUES r2; SYSTEM STOP PULLING REPLICATION LOG r2"
$CLICKHOUSE_CLIENT -q "$TRUNCATE_UNLIMITED" > "$DROP_OUT" 2>&1 &
wait_for_truncate
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${CLICKHOUSE_DATABASE} SYNC"
echo 'database dropped'
wait
# r1 can also lose the race to the shutdown latch, so require r2 as a member in any order.
grep -om1 'Code: 341.*Timeout exceeded while waiting for replicas [^.]* to process entry log-[0-9]*' "$DROP_OUT" \
    | sed 's/DB::Exception: //g; s/Received from [^ ]* //; s/log-[0-9]*/log-N/' \
    | grep -E 'replicas ([a-z0-9_]+, )*r2(, [a-z0-9_]+)* to process' \
    | sed -E 's/replicas [^ ]*(, [^ ]*)* to process/replicas r2 to process/'
