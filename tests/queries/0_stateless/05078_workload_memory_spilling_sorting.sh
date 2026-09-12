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
settings=(
  --workload "$workload"
  --max_bytes_before_external_sort 0
  --max_bytes_ratio_before_external_sort 0
  --max_threads 4
  --log_comment "$CLICKHOUSE_TEST_UNIQUE_NAME"
  --min_bytes_to_spill 0
)
$CLICKHOUSE_CLIENT -nm "${settings[@]}" -q "
CREATE OR REPLACE RESOURCE memory (MEMORY RESERVATION);
CREATE OR REPLACE WORKLOAD $workload IN $parent_workload SETTINGS max_memory = '4Gi', max_memory_before_spill = '200Mi';
SELECT number FROM numbers_mt(60e6) ORDER BY number DESC LIMIT 3 OFFSET 30e6;
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT ProfileEvents['MemoryReservationSpilledBytes'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND event_date >= yesterday()
    AND log_comment = '$CLICKHOUSE_TEST_UNIQUE_NAME'
    AND type = 'QueryFinish'
    AND query LIKE 'SELECT number FROM numbers_mt%'
"
