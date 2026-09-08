#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=workloads.lib
. "$CUR_DIR"/workloads.lib

workload=w_$CLICKHOUSE_TEST_UNIQUE_NAME
parent_workload=$(workload_ensure_root)

function cleanup()
{
  $CLICKHOUSE_CLIENT -nm -q "DROP WORKLOAD $workload" >& /dev/null || :
}
trap cleanup EXIT

# The per-operator thresholds are disabled, so the only spill trigger is the workload soft limit.
# With min_bytes_to_spill above any single aggregation state, nothing is reported as spillable,
# so the query runs over the soft limit without spilling and still finishes.
settings=(
  --workload "$workload"
  --max_bytes_before_external_group_by 0
  --max_bytes_ratio_before_external_group_by 0
  --max_threads 4
  --log_comment "$CLICKHOUSE_TEST_UNIQUE_NAME"
)
$CLICKHOUSE_CLIENT -nm "${settings[@]}" -q "
CREATE OR REPLACE RESOURCE memory (MEMORY RESERVATION);
CREATE OR REPLACE WORKLOAD $workload IN $parent_workload SETTINGS max_memory = '4Gi', max_memory_before_spill = '200Mi';
SELECT count(), sum(c) FROM (SELECT number AS k, count() AS c FROM numbers_mt(20e6) GROUP BY k) SETTINGS min_bytes_to_spill = 0;
SELECT count(), sum(c) FROM (SELECT number AS k, count() AS c FROM numbers_mt(20e6) GROUP BY k) SETTINGS min_bytes_to_spill = '10Gi';
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT Settings['min_bytes_to_spill'], ProfileEvents['MemoryReservationReclaimableBytes'] > 0, ProfileEvents['MemoryReservationSpilledBytes'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND event_date >= yesterday()
    AND log_comment = '$CLICKHOUSE_TEST_UNIQUE_NAME'
    AND type = 'QueryFinish'
    AND query LIKE 'SELECT count(), sum(c)%'
ORDER BY event_time_microseconds
"
