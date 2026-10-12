#!/usr/bin/env bash
# Tags: no-parallel
# The memory reservation resource is shared with the other workload tests.

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
    $CLICKHOUSE_CLIENT -nm -q "DROP TABLE IF EXISTS spill_result; DROP WORKLOAD IF EXISTS $workload" >/dev/null 2>&1 || :
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
$CLICKHOUSE_CLIENT -nm -q "
    CREATE WORKLOAD $workload SETTINGS max_memory = '1Gi', max_memory_before_spill = 0, max_memory_to_spill_ratio = 0;
    CREATE TABLE spill_result (number UInt64, c UInt64) ENGINE = Memory;
"

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

# One complete `Native` block; keeping the pipe open withholds EOF from `input`.
$CLICKHOUSE_CLIENT -q "SELECT number FROM numbers(262144) SETTINGS max_block_size = 262144 FORMAT Native" > "$files/block"
mkfifo "$files/input"
exec {input}<>"$files/input"
# The second branch gives the executor a spare worker while the source waits for input.
# shellcheck disable=SC2086
timeout --kill-after=5 120 $CLICKHOUSE_CLIENT --query_id "$query_id" --workload "$workload" \
    --max_threads 2 --enable_adaptive_aggregator 0 --min_bytes_to_spill 0 \
    --max_bytes_before_external_group_by 0 --max_bytes_ratio_before_external_group_by 0 \
    --max_insert_block_size 262144 --min_insert_block_size_rows 0 --min_insert_block_size_bytes 0 \
    --input_format_parallel_parsing 0 --log_profile_events 1 --receive_timeout 120 \
    -q "INSERT INTO spill_result
        SELECT number, count() FROM input('number UInt64') GROUP BY number
        UNION ALL SELECT toUInt64(262144), toUInt64(1)
        FORMAT Native" < "$files/input" {input}>&- > "$files/client.log" 2>&1 &
client_pid=$!
cat "$files/block" >&"$input"

# Publication happens after consuming the block. There is no more aggregation work until EOF.
wait_for "SELECT count() = 1 FROM system.processes WHERE query_id = '$query_id' AND ProfileEvents['MemoryReservationReclaimableBytes'] > 1048576"
$CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['MemoryReservationSpilledBytes'] = 0 FROM system.processes WHERE query_id = '$query_id'"
$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE WORKLOAD $workload SETTINGS max_memory = '1Gi', max_memory_before_spill = 1, max_memory_to_spill_ratio = 0"
wait_for "SELECT count() = 1 FROM system.processes WHERE query_id = '$query_id' AND ProfileEvents['MemoryReservationSpilledBytes'] > 0"
echo 'Spilled while waiting for input'

exec {input}>&-
input=
wait "$client_pid"
client_pid=
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(c), sum(number) FROM spill_result"
