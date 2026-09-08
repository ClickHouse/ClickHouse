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

settings=(
  --join_algorithm 'grace_hash'
  --enable_adaptive_memory_spill_scheduler 1
  --max_threads 1
  --workload "$workload"
  --grace_hash_join_initial_buckets 1
  --min_bytes_to_spill 0
)
$CLICKHOUSE_CLIENT -nm "${settings[@]}" -q "
CREATE OR REPLACE RESOURCE memory (MEMORY RESERVATION);

-- { echo }
CREATE WORKLOAD $workload IN $parent_workload SETTINGS max_memory = '1Gi', max_memory_before_spill = '500Mi';
SELECT count() FROM numbers(100e6) l INNER JOIN numbers(100e6) r USING (number) SETTINGS max_memory_usage='1Gi';

CREATE OR REPLACE WORKLOAD $workload IN $parent_workload SETTINGS max_memory = '1Gi', max_memory_before_spill = '500Mi';
SELECT * FROM numbers(100e6) l LEFT JOIN numbers(100e6) r USING (number) FORMAT Null SETTINGS max_memory_usage='500Mi'; -- { serverError MEMORY_LIMIT_EXCEEDED }

CREATE OR REPLACE WORKLOAD $workload IN $parent_workload SETTINGS max_memory = '1Gi', max_memory_before_spill = '300Mi';
SELECT count() FROM numbers(100e6) l INNER JOIN numbers(100e6) r USING (number) SETTINGS max_memory_usage='500Mi';
"
