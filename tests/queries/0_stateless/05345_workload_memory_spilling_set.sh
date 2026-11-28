#!/usr/bin/env bash
# Tags: no-parallel
# The singleton `MEMORY RESERVATION` resource is created and dropped by these tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

workload=w_$CLICKHOUSE_TEST_UNIQUE_NAME
function cleanup()
{
    $CLICKHOUSE_CLIENT -nm -q "DROP TABLE IF EXISTS set_spill; DROP WORKLOAD IF EXISTS $workload; DROP RESOURCE IF EXISTS memory" >/dev/null 2>&1 || :
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -nm -q "
CREATE OR REPLACE RESOURCE memory (MEMORY RESERVATION);
CREATE WORKLOAD $workload SETTINGS max_memory = '1Gi', max_memory_before_spill = '16Mi';
"

function run_query()
{
    # Only workload pressure may request spills. Aggregation without keys cannot spill its state.
    local settings=(
        --workload "$workload"
        --max_bytes_before_external_set 0
        --max_bytes_ratio_before_external_set 0
        --enable_adaptive_memory_spill_scheduler 0
        --enable_adaptive_aggregator 0
        --enable_parallel_replicas 0
        --use_index_for_in_with_subqueries 1
        --use_index_for_in_with_subqueries_max_values 0
        --max_rows_to_read 0
        --max_threads 4
        --max_block_size 16384
        --max_untracked_memory 0
        --min_bytes_to_spill 8Mi
        --log_profile_events 1
    )

    $CLICKHOUSE_CLIENT "${settings[@]}" --log_comment "$CLICKHOUSE_TEST_UNIQUE_NAME/$1" -q "$2"

    # `EXPLAIN ANALYZE` reports a positive spill total once, under the builder or the index-analysis consumer.
    $CLICKHOUSE_CLIENT "${settings[@]}" -q "
    WITH groupArray(explain) AS lines,
        arrayFilter(i -> lines[i] LIKE '%Spill: spilled %', arrayEnumerate(lines)) AS spills
    SELECT '$1', arrayMap(i -> trimBoth(lines[i - 1]), spills),
        arrayAll(i -> match(lines[i], 'Spill: spilled [1-9]'), spills)
    FROM (EXPLAIN ANALYZE pretty = 0, compact = 0, actions = 0, indexes = 0, description = 0 $2)
    "
}

# Construction exceeds the soft limit before the consumer starts.
run_query building "SELECT count(), sum(number) FROM numbers(131072) WHERE number IN (SELECT number FROM numbers(2097152))"

$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE WORKLOAD $workload SETTINGS max_memory = '1Gi', max_memory_before_spill = '64Mi'"

# The completed set takes 16 MiB. Pressure arrives later as the query accumulates strings;
# several consumers share the set after its builder has finished.
run_query reading "
    WITH toUInt128(number) NOT IN (SELECT toUInt128(number * 2) FROM numbers(140000)) AS keep
    SELECT countIf(keep), sumIf(number, keep), length(groupArrayIf(repeat('x', 4096), keep))
    FROM numbers_mt(81920)
"

$CLICKHOUSE_CLIENT -nm -q "
CREATE TABLE set_spill (k UInt128) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024;
INSERT INTO set_spill SELECT number FROM numbers(30000);
"

# PK analysis builds this set in a separate pipeline. The reader must keep it spillable in
# `PREWHERE`, while preserving the explicit elements used to select marks.
run_query prewhere "
    SELECT count(), sum(k), length(groupArray(repeat('x', 16384))) FROM set_spill
    PREWHERE k IN (SELECT toUInt128(if(number < 10000, number * 2, 10000000 + number)) FROM numbers(140000))
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT splitByChar('/', log_comment)[-1], count() = 1,
    max(ProfileEvents['MemoryReservationSpilledBytes']) > 0,
    max(ProfileEvents['SetsSpilledToDisk']) = 1,
    max(ProfileEvents['ExternalSetCompressedBytes']) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND event_date >= yesterday()
    AND startsWith(log_comment, '$CLICKHOUSE_TEST_UNIQUE_NAME/')
    AND type = 'QueryFinish'
    AND is_initial_query
GROUP BY log_comment
ORDER BY log_comment
"
