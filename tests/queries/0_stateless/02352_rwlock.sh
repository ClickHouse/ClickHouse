#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel -- to avoid running it in parallel, this will avoid possible issues due to high pressure

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
        if [ "$(
            $CLICKHOUSE_CLIENT -nm -q "
                system flush logs;

                select count() from system.query_log
                where
                    event_date >= yesterday() and
                    current_database = '$CLICKHOUSE_DATABASE' and
                    type = 'QueryFinish' and
                    query_id = '$query_id'
            "
        )" -eq 1 ]; then
            return 1
        else
            sleep 0.1
        fi
    done

    return 0
}

# to avoid removal via separate thread
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_ordinary Engine=Ordinary" --allow_deprecated_database_ordinary=1

# It is possible that the subsequent after INSERT query will be processed
# only after INSERT finished, it is unlikely, but it happens few times in
# debug build on CI, so if this will happen, then DROP query will be
# finished instantly, and to avoid flakiness we will retry in this case
while :; do
    $CLICKHOUSE_CLIENT -nm -q "
        DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}_ordinary.data_02352;
        CREATE TABLE ${CLICKHOUSE_DATABASE}_ordinary.data_02352 (key Int) Engine=Null();
    "

    insert_query_id="insert-$(random_str 10)"
    # 20 seconds sleep
    $CLICKHOUSE_CLIENT --query_id "$insert_query_id" -q "INSERT INTO ${CLICKHOUSE_DATABASE}_ordinary.data_02352 SELECT sleepEachRow(1) FROM numbers(20) GROUP BY number" &
    if ! wait_query_by_id_started "$insert_query_id"; then
        wait
        continue
    fi

    drop_query_id="drop-$(random_str 10)"
    # 10 second wait
    $CLICKHOUSE_CLIENT --query_id "$drop_query_id" -q "DROP TABLE ${CLICKHOUSE_DATABASE}_ordinary.data_02352 SYNC" --lock_acquire_timeout 10 > >(
        grep -m1 -o 'WRITE locking attempt on ".*" has timed out'
    ) 2>&1 &
    if ! wait_query_by_id_started "$drop_query_id"; then
        wait
        continue
    fi
    # Check INSERT query again, and retry if it does not exist.
    if ! wait_query_by_id_started "$insert_query_id"; then
        wait
        continue
    fi

    # NOTE: we need to run SELECT after DROP and
    # if the bug is there, then the query will wait 20 seconds (INSERT), instead of 10 (DROP) and will fail
    #
    # 11 seconds wait (DROP + 1 second lag)
    $CLICKHOUSE_CLIENT -q "SELECT * FROM ${CLICKHOUSE_DATABASE}_ordinary.data_02352" --lock_acquire_timeout 11

    # wait DROP and INSERT
    wait

    break
done | uniq

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${CLICKHOUSE_DATABASE}_ordinary"
