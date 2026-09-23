#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=workloads.lib
. "$CUR_DIR"/workloads.lib

workload=w_$CLICKHOUSE_TEST_UNIQUE_NAME
parent_workload=$(workload_ensure_root)
query_id=q_$CLICKHOUSE_TEST_UNIQUE_NAME

function cleanup()
{
  $CLICKHOUSE_CLIENT -nm -q "DROP WORKLOAD IF EXISTS $workload" >& /dev/null || :
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -nm -q "
CREATE OR REPLACE RESOURCE memory (MEMORY RESERVATION);
CREATE WORKLOAD $workload IN $parent_workload SETTINGS max_memory = '1Gi';
"

# The query keeps the workload's scheduler nodes alive after the workload is dropped and must
# still be able to finish.
$CLICKHOUSE_CLIENT --query_id "$query_id" --workload "$workload" --max_block_size 1 -q "SELECT sleepEachRow(1) FROM numbers(300) FORMAT Null" 2>/dev/null &

while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id'") != 1 ]]; do
  :
done

$CLICKHOUSE_CLIENT -q "DROP WORKLOAD $workload"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
wait

$CLICKHOUSE_CLIENT -q "SELECT 1"
