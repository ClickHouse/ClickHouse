#!/usr/bin/env bash
# Tags: no-fasttest, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function check_refcnt_for_table()
{
    local table=$1 && shift

    $CLICKHOUSE_CLIENT -q "system stop merges $table"
    $CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "insert into $table select number, number%4 from numbers(200)"

    local query_id
    query_id="$table-$(random_str 10)"

    SETTINGS="--format Null --max_threads 1 --max_block_size 1  --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability 0.0"

    # Notes:
    # - query may sleep 1*(200/4)=50 seconds maximum, it is enough to check system.parts
    # - "part = 1" condition should prune all parts except first
    # - max_block_size=1 with index_granularity=1 will allow to cancel the query earlier
    $CLICKHOUSE_CLIENT $SETTINGS --query_id "$query_id" -q "select sleepEachRow(1) from $table where part = 1" &
    PID=$!

    # wait for query to be started
    while [ "$($CLICKHOUSE_CLIENT -q "select count() from system.processes where query_id = '$query_id'")" -ne 1 ]; do
        sleep 0.1
    done

    # When the query only starts it execution it holds reference for each part,
    # however when it starts reading, partition pruning takes place,
    # and it should hold only parts that are required for SELECT
    #
    # But to reach partition prune the function sleepEachRow() will be executed twice,
    # so 2 seconds for sleepEachRow() and 3 seconds just to ensure that it enters the reading stage.
    sleep $((2+3))

    # NOTE: parts that are used in query will have refcount increased for each range
    $CLICKHOUSE_CLIENT -q "select table, name, refcount from system.parts where database = '$CLICKHOUSE_DATABASE' and table = '$table' and refcount > 1"

    # Kill the query gracefully.
    kill -INT $PID
    wait $PID
}

# NOTE: index_granularity=1 to cancel ASAP

$CLICKHOUSE_CLIENT -nmq "
    drop table if exists data_02340;
    create table data_02340 (key Int, part Int) engine=MergeTree() partition by part order by key settings index_granularity=1;
" || exit 1
check_refcnt_for_table data_02340

$CLICKHOUSE_CLIENT -nmq "
    drop table if exists data_02340_rep sync;
    create table data_02340_rep (key Int, part Int) engine=ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX', '1') partition by part order by key settings index_granularity=1;
" || exit 1
check_refcnt_for_table data_02340_rep

exit 0
