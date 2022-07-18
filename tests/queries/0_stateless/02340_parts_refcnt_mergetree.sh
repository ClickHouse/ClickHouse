#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function check_refcnt_for_table()
{
    local table=$1 && shift

    $CLICKHOUSE_CLIENT -q "system stop merges $table"
    $CLICKHOUSE_CLIENT -q "insert into $table select number, number%4 from numbers(200)"

    local query_id
    query_id="$table-$(random_str 10)"

    # Notes:
    # - query may sleep 0.1*(200/4)=5 seconds, it is enough to check system.parts
    # - "part = 1" condition should prune all parts except first
    $CLICKHOUSE_CLIENT --format Null --max_block_size 1 --query_id "$query_id" -q "select sleepEachRow(0.1) from $table where part = 1" &
    PID=$!

    # wait for query to be started
    while [ "$($CLICKHOUSE_CLIENT -q "select count() from system.processes where query_id = '$query_id'")" -ne 1 ]; do
        sleep 0.1
    done

    # When the query only starts it execution it holds reference for each part,
    # however when it starts reading, partition pruning takes place,
    # and it should hold only parts that are required for SELECT
    #
    # So 2 seconds delay to ensure that it goes the reading stage.
    sleep 2

    # NOTE: parts that are used in query will have refcount increased for each range
    $CLICKHOUSE_CLIENT -q "select table, name, refcount from system.parts where database = '$CLICKHOUSE_DATABASE' and table = '$table' and refcount > 1"

    kill -INT $PID
    wait $PID
}

$CLICKHOUSE_CLIENT -nmq "
    drop table if exists data_02340;
    create table data_02340 (key Int, part Int) engine=MergeTree() partition by part order by key;
"
check_refcnt_for_table data_02340

$CLICKHOUSE_CLIENT -nmq "
    drop table if exists data_02340_rep;
    create table data_02340_rep (key Int, part Int) engine=ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX', '1') partition by part order by key;
"
check_refcnt_for_table data_02340_rep

exit 0
