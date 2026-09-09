#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# Tag no-parallel: uses a PAUSEABLE failpoint; concurrent test instances would share the
# same global failpoint channel and interfere with each other's ENABLE/DISABLE sequence.
# Tag no-replicated-database: DROP TABLE is routed through the replicated DDL log, so it runs in a
# DDL worker that the client's KILL QUERY cannot reach, and on replicas where the failpoint is not
# enabled; S2 and S4 would then measure the DDL-status wait instead of the queue mutex wait.

# Test that flushing the async-insert spool of a Distributed table responds to query
# cancellation (KILL QUERY), on both entrypoints that flush it: SYSTEM FLUSH DISTRIBUTED and
# DROP TABLE. Before the fix, nothing on the shutdown-flush path consulted the kill flag, so a
# killed DDL kept sending for as long as the pending backlog took, and then dropped the table
# anyway.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

FP="distributed_async_insert_pause_before_send"
Q1="flush_dist_kill_1_${CLICKHOUSE_DATABASE}_$$"
Q2="flush_dist_kill_2_${CLICKHOUSE_DATABASE}_$$"
Q3="flush_dist_kill_3_${CLICKHOUSE_DATABASE}_$$"
Q4="flush_dist_kill_4_${CLICKHOUSE_DATABASE}_$$"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}" 2>/dev/null ||:
    for q in "$Q1" "$Q2" "$Q3" "$Q4"; do
        $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${q}' FORMAT Null" 2>/dev/null ||:
    done
    wait 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS dist_t SETTINGS ignore_drop_queries_probability = 0;
        DROP TABLE IF EXISTS dist_batched SETTINGS ignore_drop_queries_probability = 0;
        DROP TABLE IF EXISTS local_t SETTINGS ignore_drop_queries_probability = 0" 2>/dev/null ||:
}
trap cleanup EXIT

# Bounded poll until a query id is gone from system.processes.
function wait_gone()
{
    for _ in $(seq 1 60); do
        if [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$1'")" = "0" ]; then
            echo "cancelled"
            return
        fi
        sleep 0.5
    done
    echo "still-running"
}

# Bounded poll until a query id shows up in system.processes.
function wait_appear()
{
    for _ in $(seq 1 60); do
        if [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$1'")" != "0" ]; then
            return
        fi
        sleep 0.5
    done
}

# Set is_killed without waiting for the query to exit, then let the paused thread go.
function kill_release()
{
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$1' FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}"
}

function killed_with_394()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.query_log
        WHERE query_id = '$1' AND exception_code = 394 AND current_database = currentDatabase()"
}

function pending_files()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT sum(data_files) FROM system.distribution_queue
        WHERE database = currentDatabase() AND table = '$1'"
}

function table_exists()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '$1'"
}

# ignore_drop_queries_probability = 0 on every DROP: the stress runner injects 0.2, and an ignored
# S4 drop means nothing reaches the failpoint and SYSTEM WAIT FAILPOINT never returns.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS dist_t SETTINGS ignore_drop_queries_probability = 0;
    DROP TABLE IF EXISTS dist_batched SETTINGS ignore_drop_queries_probability = 0;
    DROP TABLE IF EXISTS local_t SETTINGS ignore_drop_queries_probability = 0;
    CREATE TABLE local_t (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE TABLE dist_t (n UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), local_t, rand())
        SETTINGS background_insert_batch = 0;
    CREATE TABLE dist_batched (n UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), local_t, rand())
        SETTINGS background_insert_batch = 1;
    SYSTEM STOP DISTRIBUTED SENDS dist_t;
    SYSTEM STOP DISTRIBUTED SENDS dist_batched;
"

# prefer_localhost_replica = 0 is what makes the INSERT spool to disk at all: with the default 1
# a local shard is written directly to the underlying table and no .bin file is ever created.
for i in 1 2 3 4 5; do
    $CLICKHOUSE_CLIENT --query "INSERT INTO dist_t SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 0 VALUES (${i})"
done
for i in 1 2; do
    $CLICKHOUSE_CLIENT --query "INSERT INTO dist_batched SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 0 VALUES (${i})"
done

# Preconditions: without a real spool every oracle below would pass vacuously.
echo "pending dist_t: $(pending_files dist_t)"
echo "pending dist_batched: $(pending_files dist_batched)"

# S1: a killed SYSTEM FLUSH DISTRIBUTED stops at the next file instead of draining the backlog.
# Sends stay stopped, so run() never reaches the failpoint and the pause is the flush's own.
# S1 also passes on the unfixed base: the non-batching drain connects per file and
# ConnectionEstablisher already polls cancellation there. It is a regression guard, not proof of the
# send-boundary poll; S3 is the arm for that.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${FP}"
$CLICKHOUSE_CLIENT --query_id="${Q1}" --query "SYSTEM FLUSH DISTRIBUTED dist_t" 2>/dev/null &
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
kill_release "${Q1}"
wait $! 2>/dev/null ||:
echo "S1 killed: $(killed_with_394 "${Q1}")"
echo "S1 pending dist_t: $(pending_files dist_t)"

# S2: a killed DROP TABLE escapes the queue mutex while a send still holds it. The paused sender
# keeps the mutex for the whole scenario, so leaving system.processes is only possible if the
# wait for that mutex is cancellable.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${FP}"
$CLICKHOUSE_CLIENT --query "SYSTEM START DISTRIBUTED SENDS dist_t"
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
$CLICKHOUSE_CLIENT --query_id="${Q2}" --query "DROP TABLE dist_t SETTINGS ignore_drop_queries_probability = 0" 2>/dev/null &
DROP_PID=$!
wait_appear "${Q2}"
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${Q2}' FORMAT Null"
echo "S2 drop left processlist: $(wait_gone "${Q2}")"
# Still under the pause: the spool must be intact and the table must have survived the kill.
echo "S2 dist_t exists: $(table_exists dist_t)"
echo "S2 pending dist_t kept: $(pending_files dist_t)"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}"
wait $DROP_PID 2>/dev/null ||:
echo "S2 killed: $(killed_with_394 "${Q2}")"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP DISTRIBUTED SENDS dist_t"

# S3: same as S1 on the batching path, where the send loop walks the files of one batch.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${FP}"
$CLICKHOUSE_CLIENT --query_id="${Q3}" --query "SYSTEM FLUSH DISTRIBUTED dist_batched" 2>/dev/null &
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
kill_release "${Q3}"
wait $! 2>/dev/null ||:
echo "S3 killed: $(killed_with_394 "${Q3}")"
# markAsSend only runs once the whole batch succeeds, so an aborted batch keeps both files.
echo "S3 pending dist_batched: $(pending_files dist_batched)"

# S4: one pending file, so the batch has no second iteration and no drain-loop check can fire
# after its send. Only the flush's success return is left to observe the kill.
# S3 aborted a batch whose current_batch.txt was already on disk, so this flush resends it from
# there and then pops the same two names off the pending queue, which warns once per name. Sends
# stay stopped: SYSTEM FLUSH DISTRIBUTED forces a drain regardless, and leaving them stopped keeps
# the background sender out of this scenario.
$CLICKHOUSE_CLIENT --send_logs_level=error --query "SYSTEM FLUSH DISTRIBUTED dist_batched"
$CLICKHOUSE_CLIENT --query "INSERT INTO dist_batched SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 0 VALUES (3)"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${FP}"
$CLICKHOUSE_CLIENT --query_id="${Q4}" --query "DROP TABLE dist_batched SETTINGS ignore_drop_queries_probability = 0" 2>/dev/null &
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
kill_release "${Q4}"
wait $! 2>/dev/null ||:
echo "S4 killed: $(killed_with_394 "${Q4}")"
# The file was sent, so the flush itself succeeded; the DDL still has to fail.
echo "S4 dist_batched exists: $(table_exists dist_batched)"
echo "S4 pending dist_batched: $(pending_files dist_batched)"

# S5: nothing the four interrupted flushes touched was lost.
$CLICKHOUSE_CLIENT --query "
    SYSTEM START DISTRIBUTED SENDS dist_t;
    SYSTEM START DISTRIBUTED SENDS dist_batched;
    SYSTEM FLUSH DISTRIBUTED dist_t;
    SYSTEM FLUSH DISTRIBUTED dist_batched;
"
echo "S5 rows delivered: $($CLICKHOUSE_CLIENT --query "SELECT count() FROM local_t")"
