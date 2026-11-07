#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database
# Tag no-replicated-database: DROP TABLE would run in the DDL worker, which KILL QUERY does not reach.

# KILL QUERY interrupts flushing the async-insert spool of a Distributed table,
# both for SYSTEM FLUSH DISTRIBUTED and for DROP TABLE.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

FP="distributed_async_insert_pause_before_send"
Q1="flush_dist_kill_1_${CLICKHOUSE_DATABASE}_$$"
Q2="flush_dist_kill_2_${CLICKHOUSE_DATABASE}_$$"
Q3="flush_dist_kill_3_${CLICKHOUSE_DATABASE}_$$"
QH="flush_dist_hold_${CLICKHOUSE_DATABASE}_$$"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}" 2>/dev/null ||:
    for q in "$Q1" "$Q2" "$Q3" "$QH"; do
        $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${q}' FORMAT Null" 2>/dev/null ||:
    done
    wait 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS dist_t SETTINGS ignore_drop_queries_probability = 0;
        DROP TABLE IF EXISTS dist_batched SETTINGS ignore_drop_queries_probability = 0;
        DROP TABLE IF EXISTS local_t SETTINGS ignore_drop_queries_probability = 0" 2>/dev/null ||:
}
trap cleanup EXIT

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

function wait_appear()
{
    for _ in $(seq 1 60); do
        if [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$1'")" != "0" ]; then
            return
        fi
        sleep 0.5
    done
}

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

# ignore_drop_queries_probability = 0: an ignored DROP would never reach the fail point.
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

# prefer_localhost_replica = 0, otherwise the local shard is written directly and nothing is spooled.
for i in 1 2 3 4 5; do
    $CLICKHOUSE_CLIENT --query "INSERT INTO dist_t SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 0 VALUES (${i})"
done
for i in 1 2; do
    $CLICKHOUSE_CLIENT --query "INSERT INTO dist_batched SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 0 VALUES (${i})"
done

echo "pending dist_t: $(pending_files dist_t)"
echo "pending dist_batched: $(pending_files dist_batched)"

# S1: a killed SYSTEM FLUSH DISTRIBUTED stops after the file it is sending.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${FP}"
$CLICKHOUSE_CLIENT --query_id="${Q1}" --query "SYSTEM FLUSH DISTRIBUTED dist_t" 2>/dev/null &
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
kill_release "${Q1}"
wait $! 2>/dev/null ||:
echo "S1 killed: $(killed_with_394 "${Q1}")"
echo "S1 pending dist_t: $(pending_files dist_t)"

# S2: a killed DROP TABLE stops waiting for the queue lock, which a paused flush holds,
# and keeps the table with its spool.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${FP}"
$CLICKHOUSE_CLIENT --query_id="${QH}" --query "SYSTEM FLUSH DISTRIBUTED dist_t" 2>/dev/null &
HOLD_PID=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
$CLICKHOUSE_CLIENT --query_id="${Q2}" --query "DROP TABLE dist_t SETTINGS ignore_drop_queries_probability = 0" 2>/dev/null &
DROP_PID=$!
wait_appear "${Q2}"
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${Q2}' FORMAT Null"
echo "S2 drop left processlist: $(wait_gone "${Q2}")"
echo "S2 dist_t exists: $(table_exists dist_t)"
echo "S2 pending dist_t kept: $(pending_files dist_t)"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}"
wait $DROP_PID 2>/dev/null ||:
wait $HOLD_PID 2>/dev/null ||:
echo "S2 killed: $(killed_with_394 "${Q2}")"

# S3: same as S1 with batching; the aborted batch keeps both files.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${FP}"
$CLICKHOUSE_CLIENT --query_id="${Q3}" --query "SYSTEM FLUSH DISTRIBUTED dist_batched" 2>/dev/null &
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
kill_release "${Q3}"
wait $! 2>/dev/null ||:
echo "S3 killed: $(killed_with_394 "${Q3}")"
echo "S3 pending dist_batched: $(pending_files dist_batched)"

# S4: nothing was lost. The resent batch warns about its files, which are still queued as well.
$CLICKHOUSE_CLIENT --send_logs_level=error --query "
    SYSTEM START DISTRIBUTED SENDS dist_t;
    SYSTEM START DISTRIBUTED SENDS dist_batched;
    SYSTEM FLUSH DISTRIBUTED dist_t;
    SYSTEM FLUSH DISTRIBUTED dist_batched;
"
echo "S4 rows delivered: $($CLICKHOUSE_CLIENT --query "SELECT count() FROM local_t")"
