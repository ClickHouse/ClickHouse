#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workload=w_$CLICKHOUSE_TEST_UNIQUE_NAME
query_id=q_$CLICKHOUSE_TEST_UNIQUE_NAME

function cleanup()
{
  $CLICKHOUSE_CLIENT -nm -q "DROP WORKLOAD IF EXISTS $workload" >& /dev/null || :
  $CLICKHOUSE_CLIENT -q "DROP RESOURCE IF EXISTS memory" >& /dev/null || :
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -nm -q "
CREATE OR REPLACE RESOURCE memory (MEMORY RESERVATION);
CREATE WORKLOAD $workload SETTINGS max_memory = '1Gi';
"

# The query keeps the dropped workload's queue alive. DROP WORKLOAD fails the query's memory reservation,
# so the query may end on its own before KILL.
$CLICKHOUSE_CLIENT --query_id "$query_id" --workload "$workload" --max_block_size 1 -q "SELECT sleepEachRow(1) FROM numbers(300) FORMAT Null" 2>/dev/null &

while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id'") != 1 ]]; do
  :
done

$CLICKHOUSE_CLIENT -q "DROP WORKLOAD $workload"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null SETTINGS kill_throw_if_noop = false"
wait

$CLICKHOUSE_CLIENT -q "SELECT 1"
