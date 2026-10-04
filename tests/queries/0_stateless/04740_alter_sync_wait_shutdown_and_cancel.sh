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

# The maximum entry number in the shared replication log.
log_maximum()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT log_max_index FROM system.replicas
        WHERE database = currentDatabase() AND table = 'r2'"
}

# Waits until the log maximum advances past $1 and prints the new value, which names the entry the
# statement under test created. $1 has to be sampled before that statement starts: its process list
# row appears before the interpreter creates the log node, so a value read after wait_for_truncate
# can still be the previous maximum. Comparing log_pointer against a freshly read log_max_index
# instead would prove nothing: log_pointer is the maximum copied entry plus one and may point at an
# entry that does not exist yet.
wait_for_log_entry()
{
    local current
    for _ in {1..600}; do
        current=$(log_maximum)
        if [ -n "$current" ] && [ "$current" -gt "$1" ]; then
            echo "$current"
            return 0
        fi
        sleep 0.5
    done
}

# 1. max_execution_time must terminate the wait.
$CLICKHOUSE_CLIENT -q "
    TRUNCATE TABLE r1 SETTINGS alter_sync = 2, max_execution_time = 5,
        replication_wait_for_inactive_replica_timeout = -1
" 2>&1 | grep -om1 'Code: 159.*Timeout exceeded: elapsed [0-9.]* ms, maximum: 5000.000 ms' \
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
before=$(log_maximum)
$CLICKHOUSE_CLIENT -q "$TRUNCATE_UNLIMITED" > "$QUEUE_OUT" 2>&1 &
wait_for_truncate
# Reaching the third stage requires this TRUNCATE's own entry to be in r2's queue.
entry_index=$(wait_for_log_entry "$before")
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
before=$(log_maximum)
$CLICKHOUSE_CLIENT -q "$TRUNCATE_UNLIMITED" > "$DROP_OUT" 2>&1 &
wait_for_truncate
# The shutdown must not reach the table before the statement has its log entry: a TRUNCATE that has
# not passed its readonly checks yet is refused with TABLE_IS_READ_ONLY and never enters the wait.
if [ -z "$(wait_for_log_entry "$before")" ]; then
    echo 'log entry for the TRUNCATE never appeared'
fi
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${CLICKHOUSE_DATABASE} SYNC"
echo 'database dropped'
wait
# The local replica has its own latch, so only a remote unfinished replica shows that the shutdown
# stopped the wait: require r2 in the reported set, in any order and possibly alongside r1.
timeout_line=$(grep -om1 'Code: 341.*Timeout exceeded while waiting for replicas [^.]* to process entry log-[0-9]*' "$DROP_OUT" \
    | sed 's/DB::Exception: //g; s/Received from [^ ]* //; s/log-[0-9]*/log-N/' \
    | grep -E 'replicas ([a-z0-9_]+, )*r2(, [a-z0-9_]+)* to process' \
    | sed -E 's/replicas [^ ]*(, [^ ]*)* to process/replicas r2 to process/')
if [ -n "$timeout_line" ]; then
    echo "$timeout_line"
else
    reported=$(grep -om1 'Code: [0-9]*' "$DROP_OUT")
    echo "the parked TRUNCATE reported ${reported:-no error}, not the expected r2 timeout"
fi
