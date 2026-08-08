#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: `UNDROP TABLE` busy-waits until the dropped table's storage pointer becomes unique.
# If another query still holds the storage, this wait used to ignore `KILL QUERY` and could hang for
# an unbounded time. Check that `KILL QUERY` interrupts the wait with `QUERY_WAS_CANCELLED` and that
# the table can be attached afterwards (its metadata has already been returned to the database).

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_undrop_kill;
    CREATE TABLE t_undrop_kill (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_undrop_kill SELECT number FROM numbers(10);
"

select_query_id="undrop-holder-${CLICKHOUSE_DATABASE}-$RANDOM"
undrop_query_id="undrop-query-${CLICKHOUSE_DATABASE}-$RANDOM"

# Clean up unconditionally: if any assertion below fails mid-way, the background queries must be
# killed and the table dropped, or it would linger in the shared stateless database and break
# unrelated tests.
function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id IN ('$select_query_id', '$undrop_query_id') SYNC FORMAT Null"
    wait
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_undrop_kill SYNC"
}
trap cleanup EXIT

# A long-running SELECT keeps a reference to the storage. It is killed at the end of the test,
# so its "query was cancelled" error is expected and suppressed.
${CLICKHOUSE_CLIENT} --query_id "$select_query_id" --function_sleep_max_microseconds_per_block 60000000 --query "
    SELECT sleepEachRow(3) FROM t_undrop_kill FORMAT Null
" >/dev/null 2>&1 &
select_pid=$!

# Wait until the SELECT is registered and executing.
for _ in {1..100}
do
    result=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$select_query_id'")
    [[ $result == "1" ]] && break
    sleep 0.3
done

# The test config sets database_atomic_wait_for_drop_and_detach_synchronously = 1, which would make
# this DROP wait for the SELECT to release the storage and finally drop the table, leaving nothing
# to undrop. Disable it so the DROP returns while the storage is still referenced.
${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously=0 --query "DROP TABLE t_undrop_kill"

# UNDROP moves the metadata back, then busy-waits for the storage pointer held by the SELECT.
${CLICKHOUSE_CLIENT} --query_id "$undrop_query_id" --query "UNDROP TABLE t_undrop_kill" 2>&1 \
    | grep -o -m 1 'QUERY_WAS_CANCELLED' &
undrop_pid=$!

# Wait until the UNDROP is registered, then kill it.
for _ in {1..100}
do
    result=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$undrop_query_id'")
    [[ $result == "1" ]] && break
    sleep 0.3
done

${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$undrop_query_id' SYNC FORMAT Null"
wait $undrop_pid

# Release the storage reference.
${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$select_query_id' SYNC FORMAT Null"
wait $select_pid || true

# The metadata has been returned to the database before the wait, so the table can be attached.
# The cleanup trap drops the table.
${CLICKHOUSE_CLIENT} --query "
    ATTACH TABLE t_undrop_kill;
    SELECT count() FROM t_undrop_kill;
"
