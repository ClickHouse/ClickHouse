#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `UNDROP TABLE` waits until the running queries release the dropped table. Check that this wait
# respects `max_execution_time` and that the table can be attached afterwards.

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_undrop_timeout SYNC;
    CREATE TABLE t_undrop_timeout (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_undrop_timeout SELECT number FROM numbers(10);
"

select_query_id="undrop-timeout-holder-${CLICKHOUSE_DATABASE}-$RANDOM"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$select_query_id' SYNC FORMAT Null"
    wait
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_undrop_timeout SYNC"
}
trap cleanup EXIT

# A long-running SELECT keeps a reference to the storage. It is killed at the end of the test,
# so its "query was cancelled" error is expected and suppressed.
# The settings are pinned because clickhouse-test randomizes them: with parallel replicas the
# SELECT does not hold the local storage, and a randomized time limit would end it too early.
${CLICKHOUSE_CLIENT} --query_id "$select_query_id" --function_sleep_max_microseconds_per_block 60000000 \
    --enable_parallel_replicas 0 --max_execution_time 0 --query "
    SELECT sleepEachRow(3) FROM t_undrop_timeout FORMAT Null
" >/dev/null 2>&1 &

# Without a running holder the UNDROP below would return instantly and the test would prove nothing.
result=0
for _ in {1..100}
do
    result=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$select_query_id'")
    [[ $result == "1" ]] && break
    sleep 0.3
done
[[ $result == "1" ]] || { echo "the SELECT holding the storage is not running"; exit 1; }

# The test config sets database_atomic_wait_for_drop_and_detach_synchronously = 1, which would make
# this DROP wait for the SELECT and finally drop the table, leaving nothing to undrop.
${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously=0 --query "DROP TABLE t_undrop_timeout"

# UNDROP moves the metadata back, then waits for the storage pointer held by the SELECT.
${CLICKHOUSE_CLIENT} --max_execution_time 3 --query "UNDROP TABLE t_undrop_timeout" 2>&1 | grep -o -m 1 'TIMEOUT_EXCEEDED'

# Release the storage reference.
${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$select_query_id' SYNC FORMAT Null"
wait

# The metadata has been returned to the database before the wait, so the table can be attached.
${CLICKHOUSE_CLIENT} --query "
    ATTACH TABLE t_undrop_timeout;
    SELECT count() FROM t_undrop_timeout;
"
