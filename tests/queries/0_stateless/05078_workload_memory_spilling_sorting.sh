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
settings=(
  --workload "$workload"
  --max_rows_to_read 0
  --max_bytes_before_external_sort 0
  --max_bytes_ratio_before_external_sort 0
  --max_threads 4
  --log_comment "$CLICKHOUSE_TEST_UNIQUE_NAME"
  --min_bytes_to_spill 8Mi
)
$CLICKHOUSE_CLIENT -nm "${settings[@]}" -q "
CREATE OR REPLACE RESOURCE memory (MEMORY RESERVATION);
CREATE OR REPLACE WORKLOAD $workload SETTINGS max_memory = '4Gi', max_memory_before_spill = '50Mi';
SELECT number FROM numbers_mt(15e6) ORDER BY number DESC LIMIT 3 OFFSET 7.5e6;
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
