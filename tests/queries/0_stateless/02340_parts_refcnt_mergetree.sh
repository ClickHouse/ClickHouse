#!/usr/bin/env bash
# Tags: no-fasttest, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function check_refcnt_for_table()
{
    local table=$1 && shift

    $CLICKHOUSE_CLIENT -m -q "
        system stop merges $table;
        -- cleanup thread may hold the parts lock
        system stop cleanup $table;
        -- queue may hold the parts lock for awhile as well
        system stop pulling replication log $table;
    "
    $CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "insert into $table select number, number%4 from numbers(2000)"

    local query_id
    query_id="$table-$(random_str 10)"

    local log_file
    log_file=$(mktemp "$CUR_DIR/clickhouse-tests.XXXXXX.log")
    local args=(
        --allow_repeated_settings
        --format Null
        --max_threads 1
        --max_block_size 1
        --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability 0.0
        --query_id "$query_id"
        --send_logs_level "test"
        --server_logs_file "$log_file"
    )

    # Notes:
    # - query may sleep 0.1*(2000/4)=50 seconds maximum, it is enough to check system.parts
    # - "part = 1" condition should prune all parts except first
    # - max_block_size=1 with index_granularity=1 will allow to cancel the query earlier
    $CLICKHOUSE_CLIENT "${args[@]}" -q "select sleepEachRow(0.1) from $table where part = 1" &
    PID=$!

    # When the query only starts it execution it holds reference for each part,
    # however when it starts reading, partition pruning takes place,
    # and it should hold only parts that are required for SELECT
    #
    # So let's wait while the reading will be started.
    while ! grep -F -q -e "Exception" -e "MergeTreeRangeReader" "$log_file"; do
        sleep 0.1
    done

    # NOTE: parts that are used in query will be holded in multiple places, and
    # this is where magic 6 came from. Also there could be some other
    # background threads (i.e. asynchronous metrics) that uses the part, so we
    # simply filter parts not by "refcount > 1" but with some delta - "3", to
    # avoid flakiness.
    $CLICKHOUSE_CLIENT -q "select table, name, refcount>=6 from system.parts where database = '$CLICKHOUSE_DATABASE' and table = '$table' and refcount >= 3"

    # Kill the query gracefully.
    kill -INT $PID
    wait $PID
    grep -F Exception "$log_file" | grep -v -F QUERY_WAS_CANCELLED
    rm -f "${log_file:?}"
}

# NOTE: index_granularity=1 to cancel ASAP

$CLICKHOUSE_CLIENT -mq "
    drop table if exists data_02340;
    create table data_02340 (key Int, part Int) engine=MergeTree() partition by part order by key settings index_granularity=1;
" || exit 1
check_refcnt_for_table data_02340
$CLICKHOUSE_CLIENT -q "drop table data_02340 sync"

$CLICKHOUSE_CLIENT -mq "
    drop table if exists data_02340_rep sync;
    create table data_02340_rep (key Int, part Int) engine=ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX', '1') partition by part order by key settings index_granularity=1;
" || exit 1
check_refcnt_for_table data_02340_rep
$CLICKHOUSE_CLIENT -q "drop table data_02340_rep sync"

exit 0
