#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: checks thread count, which can be affected by concurrent queries

# Demonstrates that a plain INSERT (no SELECT, no MVs) with high max_threads
# eagerly spawns many threads even though the pipeline is 1-wide and can only
# use 1 thread. This happens because ConcurrencyControl grants slots eagerly
# and spawnThreads keeps spawning until it runs out of granted slots.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_insert_threads"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_insert_threads (x UInt64) ENGINE = MergeTree ORDER BY x"

# Plain INSERT FORMAT TSV with max_threads=16, no MVs.
# The insert pipeline is a single chain — only needs 1 thread.
QUERY_ID="04102_plain_insert_$RANDOM"

$CLICKHOUSE_CLIENT -q "SELECT number FROM numbers(10000) FORMAT TSV" | \
$CLICKHOUSE_CLIENT \
    --query_id="$QUERY_ID" \
    --max_threads=16 \
    --max_insert_threads=1 \
    --log_queries=1 \
    --send_logs_level=trace \
    -q "INSERT INTO test_insert_threads FORMAT TSV" 2>"${CLICKHOUSE_TMP}/04102_insert_log.txt"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

# Check peak_threads_usage: with max_threads=16, the pipeline only needs 1 thread
# but due to eager CC slot granting and eager thread spawning, many more are created.
$CLICKHOUSE_CLIENT -q "
    SELECT
        if(peak_threads_usage > 4, 'MANY THREADS SPAWNED', 'FEW THREADS')
    FROM system.query_log
    WHERE event_date >= yesterday()
        AND event_time >= now() - 600
        AND current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_id = '$QUERY_ID'
"

# Confirm that CC allocation requested min=1, max=16 (full max_threads budget)
grep -c 'Allocating CPU slots from ConcurrencyControl: min=1, max=16' "${CLICKHOUSE_TMP}/04102_insert_log.txt"

# Confirm that multiple threads were spawned with SHOULD_SPAWN before stopping
EAGER_SPAWNS=$(grep -c 'spawnThreads: spawning thread.*spawn_status=SHOULD_SPAWN' "${CLICKHOUSE_TMP}/04102_insert_log.txt")
if [ "$EAGER_SPAWNS" -gt 4 ]; then
    echo "MORE THAN 4 EAGER SPAWNS"
else
    echo "FEW SPAWNS"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE test_insert_threads"
