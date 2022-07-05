#!/usr/bin/env bash

# Test that ensures that WRITE lock failure notifies READ.
# In other words to ensure that after WRITE lock failure (DROP),
# READ lock (SELECT) available instantly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function wait_query_by_id_started()
{
    local query_id=$1 && shift
    # wait for query to be started
    while [ "$($CLICKHOUSE_CLIENT "$@" -q "select count() from system.processes where query_id = '$query_id'")" -ne 1 ]; do
        sleep 0.1
    done
}

# to avoid removal via separate thread
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_ordinary Engine=Ordinary" --allow_deprecated_database_ordinary=1
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_ordinary.data_02352 (key Int) Engine=Null()"

query_id="insert-$(random_str 10)"
# 20 seconds sleep
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "INSERT INTO ${CLICKHOUSE_DATABASE}_ordinary.data_02352 SELECT sleepEachRow(1) FROM numbers(20) GROUP BY number" &
wait_query_by_id_started "$query_id"

query_id="drop-$(random_str 10)"
# 10 second wait
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "DROP TABLE ${CLICKHOUSE_DATABASE}_ordinary.data_02352 SYNC" --lock_acquire_timeout 10 > >(grep -m1 -o 'WRITE locking attempt on ".*" has timed out') 2>&1 &
wait_query_by_id_started "$query_id"

# NOTE: we need to run SELECT after DROP and
# if the bug is there, then the query will wait 20 seconds (INSERT), instead of 10 (DROP) and will fail
#
# 11 seconds wait (DROP + 1 second lag)
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${CLICKHOUSE_DATABASE}_ordinary.data_02352" --lock_acquire_timeout 11

# wait DROP and INSERT
wait

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${CLICKHOUSE_DATABASE}_ordinary"
