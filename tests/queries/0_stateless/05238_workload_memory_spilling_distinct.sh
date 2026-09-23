#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=workloads.lib
. "$CUR_DIR"/workloads.lib

set -e

workload=w_$CLICKHOUSE_TEST_UNIQUE_NAME
parent_workload=$(workload_ensure_root)

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $workload" >& /dev/null || :
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -nm -q "
CREATE OR REPLACE RESOURCE memory (MEMORY RESERVATION);
CREATE WORKLOAD $workload IN $parent_workload SETTINGS max_memory = '2Gi', max_memory_before_spill = '16Mi';
"

# Only workload pressure can trigger spilling; the per-query thresholds and adaptive scheduler are disabled.
settings=(
    --workload "$workload"
    --max_bytes_before_external_distinct 0
    --max_bytes_ratio_before_external_distinct 0
    --max_bytes_before_external_sort 0
    --max_bytes_ratio_before_external_sort 0
    --enable_adaptive_memory_spill_scheduler 0
    --allow_preliminary_distinct_abandoning 0
    --optimize_distinct_in_order 0
    --enable_parallel_replicas 0
    --max_threads 1
    --max_block_size 32768
    --max_untracked_memory 0
    --min_bytes_to_spill 1048576
)

function run_query()
{
    $CLICKHOUSE_CLIENT "${settings[@]}" --log_comment "$CLICKHOUSE_TEST_UNIQUE_NAME/$1" -q "$2"
}

# Repeated keys cross the transition from hashing to suppression files and ordinary spill runs.
run_query typed "
SELECT count() = 1048576, sum(k) = 549755289600
FROM (SELECT DISTINCT number % 1048576 AS k FROM numbers(6291456))
"

# A single input block retains more than one suppression run's worth of string keys.
run_query strings "
SELECT count() = 65536, sum(toUInt64(splitByChar(':', k)[1])) = 2147450880
FROM (SELECT DISTINCT concat(toString(number % 65536), ':', repeat('x', 1024)) AS k FROM numbers(262144))
"

# Generic keys retain fingerprints; suppression and ordinary runs have different column layouts.
run_query fingerprints "
SELECT count() = 1048576, sum(k[1]) = 549755289600
FROM (SELECT DISTINCT [number % 1048576] AS k FROM numbers(4194304))
"

# Preliminary sets may be released too; final deduplication must still remove every duplicate.
run_query parallel "
SELECT count() = 1048576, sum(k) = 549755289600
FROM (SELECT DISTINCT number % 1048576 AS k FROM numbers_mt(8388608))
SETTINGS max_threads = 4
"

# The final `DISTINCT` follows an expression sort and must restore arrival order after spilling.
run_query ordered "
SELECT count() = 1048576, sum(k) = 549755289600, groupArray(k) = arrayReverseSort(groupArray(k))
FROM (SELECT DISTINCT number % 1048576 AS k FROM numbers(4194304) ORDER BY k + 1 DESC)
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT splitByChar('/', log_comment)[-1], count() = 1,
       max(ProfileEvents['MemoryReservationSpilledBytes']) > 0,
       max(ProfileEvents['ExternalDistinctUncompressedBytes']) > 0,
       max(ProfileEvents['ExternalDistinctMerge']) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND event_date >= yesterday()
    AND startsWith(log_comment, '$CLICKHOUSE_TEST_UNIQUE_NAME/')
    AND type = 'QueryFinish'
    AND is_initial_query
GROUP BY log_comment
ORDER BY log_comment
"
