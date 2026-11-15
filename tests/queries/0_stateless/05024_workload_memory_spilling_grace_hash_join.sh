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

settings=(
  --join_algorithm 'grace_hash'
  --max_threads 1
  --workload "$workload"
  --max_rows_to_read 0
  --grace_hash_join_initial_buckets 1
  --min_bytes_to_spill 20Mi
)
$CLICKHOUSE_CLIENT -nm "${settings[@]}" -q "
CREATE OR REPLACE RESOURCE memory (MEMORY RESERVATION);

-- { echo }
CREATE WORKLOAD $workload SETTINGS max_memory = '1Gi', max_memory_before_spill = '500Mi';
SELECT count() FROM numbers(20e6) l INNER JOIN numbers(20e6) r USING (number) SETTINGS max_memory_usage='1Gi';

CREATE OR REPLACE WORKLOAD $workload SETTINGS max_memory = '1Gi', max_memory_before_spill = '500Mi';
SELECT * FROM numbers(20e6) l LEFT JOIN numbers(20e6) r USING (number) FORMAT Null SETTINGS max_memory_usage='500Mi'; -- { serverError MEMORY_LIMIT_EXCEEDED }

CREATE OR REPLACE WORKLOAD $workload SETTINGS max_memory = '1Gi', max_memory_before_spill = '200Mi';
SELECT count() FROM numbers(20e6) l INNER JOIN numbers(20e6) r USING (number) SETTINGS max_memory_usage='500Mi';
"
