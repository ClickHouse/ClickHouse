#!/usr/bin/env bash
# Tags: no-parallel
# The singleton memory reservation resource is shared with the other workload tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

workload=w_$CLICKHOUSE_TEST_UNIQUE_NAME
query_id=$CLICKHOUSE_TEST_UNIQUE_NAME
files=$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME
mkdir -p "$files"
resource=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.resources WHERE unit = 'MemoryByte'")
created_resource=
client_pid=
function cleanup()
{
    if [[ -n ${input:-} ]]; then
        exec {input}>&-
    fi
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC" >/dev/null 2>&1 || :
    if [[ -n $client_pid ]]; then
        kill "$client_pid" 2>/dev/null || :
        wait "$client_pid" 2>/dev/null || :
    fi
    $CLICKHOUSE_CLIENT -nm -q "DROP TABLE IF EXISTS spill_kept_keys; DROP WORKLOAD IF EXISTS $workload" >/dev/null 2>&1 || :
    if [[ -n $created_resource ]]; then
        $CLICKHOUSE_CLIENT -q "DROP RESOURCE $resource" >/dev/null 2>&1 || :
    fi
    rm -rf "$files"
}
trap cleanup EXIT

if [[ -z $resource ]]; then
    resource=memory_$CLICKHOUSE_TEST_UNIQUE_NAME
    $CLICKHOUSE_CLIENT -q "CREATE RESOURCE $resource (MEMORY RESERVATION)"
    created_resource=1
fi
$CLICKHOUSE_CLIENT -q "CREATE TABLE spill_kept_keys (k UInt64, n UInt64, payload_ok UInt8) ENGINE = Memory"

function wait_for()
{
    local deadline=$((SECONDS + 60))
    until [[ $($CLICKHOUSE_CLIENT -q "$1") == 1 ]]; do
        if (( SECONDS >= deadline )) || ! kill -0 "$client_pid" 2>/dev/null; then
            echo "Timeout or query failure waiting for: $1" >&2
            cat "$files/client.log" >&2
            exit 1
        fi
        sleep 0.1
    done
}

# Each file is one complete `Native` block. Large aggregate states remain spillable after the cutoff.
for keys in 16 64; do
    $CLICKHOUSE_CLIENT -q "SELECT toUInt64(number % $keys) AS k, repeat('x', 512) AS v
        FROM numbers(16384) SETTINGS max_block_size = 16384 FORMAT Native" > "$files/block_$keys"
done
mkfifo "$files/input"

# The first block exceeds `LIMIT 8`, but not `LIMIT 32`: exercise both sides of the cutoff arbitration.
for limit in 8 32; do
    query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_$limit
    $CLICKHOUSE_CLIENT -nm -q "
        TRUNCATE TABLE spill_kept_keys;
        CREATE OR REPLACE WORKLOAD $workload SETTINGS max_memory = '1Gi', max_memory_before_spill = 0, max_memory_to_spill_ratio = 0;
    "
    exec {input}<>"$files/input"
    # The second branch leaves a worker available while `input` waits for the next block.
    # shellcheck disable=SC2086
    timeout --kill-after=5 120 $CLICKHOUSE_CLIENT --query_id "$query_id" --workload "$workload" \
        --enable_parallel_replicas 0 --optimize_trivial_group_by_limit_query 1 \
        --enable_group_by_top_k_optimization 0 --enable_adaptive_aggregator 0 --enable_adaptive_memory_spill_scheduler 0 \
        --max_threads 2 --min_bytes_to_spill 2Mi --max_block_size 16384 \
        --max_bytes_before_external_group_by 0 --max_bytes_ratio_before_external_group_by 0 \
        --max_insert_block_size 16384 --min_insert_block_size_rows 0 --min_insert_block_size_bytes 0 \
        --input_format_parallel_parsing 0 --log_queries 1 --log_profile_events 1 --receive_timeout 120 \
        -q "INSERT INTO spill_kept_keys
            SELECT k, length(values), arrayAll(x -> x = repeat('x', 512), values)
            FROM (SELECT k, groupArray(v) AS values FROM input('k UInt64, v String') GROUP BY k LIMIT $limit)
            UNION ALL SELECT toUInt64(64), toUInt64(0), toUInt8(1)
            FORMAT Native" < "$files/input" {input}>&- > "$files/client.log" 2>&1 &
    client_pid=$!
    cat "$files/block_16" >&"$input"
    wait_for "SELECT count() = 1 FROM system.processes WHERE query_id = '$query_id'
        AND ProfileEvents['MemoryReservationReclaimableBytes'] >= 2097152"
    $CLICKHOUSE_CLIENT -q "CREATE OR REPLACE WORKLOAD $workload SETTINGS max_memory = '1Gi', max_memory_before_spill = 1, max_memory_to_spill_ratio = 0"
    wait_for "SELECT count() = 1 FROM system.processes WHERE query_id = '$query_id'
        AND ProfileEvents['MemoryReservationSpilledBytes'] > 0"
    $CLICKHOUSE_CLIENT -q "CREATE OR REPLACE WORKLOAD $workload SETTINGS max_memory = '1Gi', max_memory_before_spill = 0, max_memory_to_spill_ratio = 0"
    cat "$files/block_64" >&"$input"
    exec {input}>&-
    input=
    if ! wait "$client_pid"; then
        cat "$files/client.log" >&2
        exit 1
    fi
    client_pid=

    # Every retained key must include its rows from both sides of the spill.
    $CLICKHOUSE_CLIENT -q "SELECT $limit, count() = $limit, countIf(n != if(k < 16, 1280, 256)) = 0, min(payload_ok)
        FROM spill_kept_keys WHERE k < 64"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT -q "SELECT count() = 1,
        (max(ProfileEvents['OverflowAny']) > 0) = ($limit = 8),
        (max(ProfileEvents['AggregationSharedKeptKeysSpillReseeds']) > 0) = ($limit = 8)
        FROM system.query_log WHERE event_date >= yesterday() AND query_id = '$query_id' AND type = 'QueryFinish' AND current_database = currentDatabase()"
done
