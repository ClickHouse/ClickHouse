#!/usr/bin/env bash
# Tags: long, no-parallel
# The singleton `MEMORY RESERVATION` resource is created and dropped by these tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workload=w_$CLICKHOUSE_TEST_UNIQUE_NAME

function cleanup()
{
  $CLICKHOUSE_CLIENT -nm -q "DROP WORKLOAD $workload" >& /dev/null || :
  $CLICKHOUSE_CLIENT -q "DROP RESOURCE IF EXISTS memory" >& /dev/null || :
}
trap cleanup EXIT

# The per-operator thresholds are disabled, so the only spill trigger is the workload soft limit.
# The adaptive aggregation stays enabled: its spill requests are served by draining the staged backlog.
settings=(
  --workload "$workload"
  --max_rows_to_read 0
  --max_bytes_before_external_group_by 0
  --max_bytes_ratio_before_external_group_by 0
  --max_threads 4
  --min_bytes_to_spill 0
)
$CLICKHOUSE_CLIENT --enable_adaptive_aggregator 1 -nm "${settings[@]}" --log_comment "$CLICKHOUSE_TEST_UNIQUE_NAME" -q "
CREATE OR REPLACE RESOURCE memory (MEMORY RESERVATION);
CREATE OR REPLACE WORKLOAD $workload SETTINGS max_memory = '4Gi', max_memory_before_spill = '200Mi';
SELECT count(), sum(c) FROM (SELECT number AS k, count() AS c FROM numbers_mt(20e6) GROUP BY k) SETTINGS enable_adaptive_aggregator=1;
SELECT count(), sum(c) FROM (SELECT number AS k, count() AS c FROM numbers_mt(20e6) GROUP BY k) SETTINGS enable_adaptive_aggregator=0;
"

# Fewer than one million keys can never fill an adaptive spill part with the query threshold disabled.
# A workload request must write this tail to disk instead of hiding it in the shared drain table.
$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE WORKLOAD $workload SETTINGS max_memory = '4Gi', max_memory_before_spill = '32Mi'"
$CLICKHOUSE_CLIENT "${settings[@]}" --log_comment "$CLICKHOUSE_TEST_UNIQUE_NAME/tail" -q "
SELECT count(), sum(c) FROM
(
    SELECT concat(toString(number), repeat('x', 512)) AS k, count() AS c
    FROM numbers_mt(262144) GROUP BY k
)
SETTINGS enable_adaptive_aggregator = 1, enable_adaptive_memory_spill_scheduler = 0,
    adaptive_aggregator_freeze_threshold = 1000, adaptive_aggregator_freeze_threshold_bytes = 0,
    collect_hash_table_stats_during_aggregation = 0, max_threads = 2, max_block_size = 8192
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT ProfileEvents['MemoryReservationSpilledBytes'] > 0, ProfileEvents['AdaptiveAggregationSpillDrains'] > 0, Settings['enable_adaptive_aggregator']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND event_date >= yesterday()
    AND log_comment = '$CLICKHOUSE_TEST_UNIQUE_NAME'
    AND type = 'QueryFinish'
    AND query LIKE 'SELECT count(), sum(c)%'
ORDER BY event_time_microseconds
"

$CLICKHOUSE_CLIENT -q "
SELECT 'tail spilled', count() = 1,
    max(ProfileEvents['AdaptiveAggregationSpillDrains']) > 0,
    max(ProfileEvents['AdaptiveAggregationSharedTableSpills']) > 0,
    max(ProfileEvents['ExternalAggregationCompressedBytes']) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND event_date >= yesterday()
    AND log_comment = '$CLICKHOUSE_TEST_UNIQUE_NAME/tail'
    AND type = 'QueryFinish'
"
