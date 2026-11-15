#!/usr/bin/env bash
# Tags: no-parallel
# The singleton memory reservation resource is shared with the other workload tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

workload=w_$CLICKHOUSE_TEST_UNIQUE_NAME
resource=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.resources WHERE unit = 'MemoryByte'")
created_resource=
function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $workload" >/dev/null 2>&1 || :
    if [[ -n $created_resource ]]; then
        $CLICKHOUSE_CLIENT -q "DROP RESOURCE $resource" >/dev/null 2>&1 || :
    fi
}
trap cleanup EXIT

if [[ -z $resource ]]; then
    resource=memory_$CLICKHOUSE_TEST_UNIQUE_NAME
    $CLICKHOUSE_CLIENT -q "CREATE RESOURCE $resource (MEMORY RESERVATION)"
    created_resource=1
fi
$CLICKHOUSE_CLIENT -q "CREATE WORKLOAD $workload SETTINGS max_memory = '1Gi', max_memory_before_spill = '16Mi'"

# Frozen tables keep growing through local hits, without staging any records. Only workload pressure
# can spill them, and the small states built before freezing are below `min_bytes_to_spill`. A spill
# request writes a frozen table to disk, and its producer learns its keys again.
settings=(
    --workload "$workload"
    --enable_adaptive_aggregator 1
    --enable_adaptive_memory_spill_scheduler 0
    --adaptive_aggregator_freeze_threshold_bytes 0
    --collect_hash_table_stats_during_aggregation 0
    --use_aggregation_memory_tracker 1
    --max_bytes_before_external_group_by 0
    --max_bytes_ratio_before_external_group_by 0
    --min_bytes_to_spill 4Mi
    --max_threads 2
    --max_block_size 8192
    --max_untracked_memory 0
    --enable_parallel_replicas 0
    --optimize_group_by_constant_keys 1
    --log_queries 1
    --log_profile_events 1
)

$CLICKHOUSE_CLIENT "${settings[@]}" --log_comment "$CLICKHOUSE_TEST_UNIQUE_NAME/batch" -q "
SELECT sum(length(a)) = 4194304, sum(arraySum(a)) = 8796090925056
FROM (SELECT toUInt64(number % 2) AS k, groupArray(number) AS a FROM numbers_mt(4194304) GROUP BY k)
SETTINGS adaptive_aggregator_freeze_threshold = 2
"

# `ignore` produces a runtime constant column, exercising the single-place aggregate update.
$CLICKHOUSE_CLIENT "${settings[@]}" --log_comment "$CLICKHOUSE_TEST_UNIQUE_NAME/constant" -q "
SELECT sum(length(a)) = 4194304, sum(arraySum(a)) = 8796090925056
FROM (SELECT toUInt64(ignore(number)) AS k, groupArray(number) AS a FROM numbers_mt(4194304) GROUP BY k)
SETTINGS adaptive_aggregator_freeze_threshold = 1
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT splitByChar('/', log_comment)[-1], count() = 1,
    max(ProfileEvents['AdaptiveAggregationLocalFreezes']) > 0,
    max(ProfileEvents['AdaptiveAggregationStagedRecords']) = 0,
    max(ProfileEvents['AdaptiveAggregationFrozenTableSpills']) > 0,
    max(ProfileEvents['MemoryReservationSpilledBytes']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday()
    AND startsWith(log_comment, '$CLICKHOUSE_TEST_UNIQUE_NAME/') AND type = 'QueryFinish' AND is_initial_query
GROUP BY log_comment ORDER BY log_comment
"
