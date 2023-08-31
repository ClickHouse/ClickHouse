#!/usr/bin/env bash
# Tags: distributed

# shellcheck disable=SC2206

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# -- NOTE: this test cannot use 'current_database = $CLICKHOUSE_DATABASE',
# -- because it does not propagated via remote queries,
# -- hence it uses query_id/initial_query_id.

function setup()
{
    $CLICKHOUSE_CLIENT -nm -q "
        drop table if exists data_01814;
        drop table if exists dist_01814;

        create table data_01814 (key Int) Engine=MergeTree() order by key settings index_granularity=10 as select * from numbers(100);
        create table dist_01814 as data_01814 engine=Distributed('test_cluster_two_shards', $CLICKHOUSE_DATABASE, data_01814, key);
    "
}

function cleanup()
{
    $CLICKHOUSE_CLIENT -nm -q "
        drop table data_01814;
        drop table dist_01814;
    "
}

function make_query_id()
{
    echo "$(tr -cd '[:lower:]' < /dev/urandom | head -c10)-$CLICKHOUSE_DATABASE"
}

function test_distributed_push_down_limit_with_query_log()
{
    local table=$1 && shift
    local offset=$1 && shift
    local query_id

    query_id="$(make_query_id)"

    # NOTES:
    # - max_rows_to_read_leaf cannot be used since it does not know anything
    #   about optimize_aggregation_in_order,
    # - limit push down can be checked only with optimize_aggregation_in_order,
    #   since otherwise the query will be canceled too early, and read_rows will be
    #   small.
    local settings_and_opts=(
        --query_id "$query_id"

        --max_block_size 20
        --optimize_aggregation_in_order 1
        --log_queries 1
        --log_queries_min_type 'QUERY_FINISH'

        # disable hedged requests to avoid excessive log entries
        --use_hedged_requests 0

        "$@"
    )

    $CLICKHOUSE_CLIENT "${settings_and_opts[@]}" -q "select * from $table group by key limit $offset, 10"

    $CLICKHOUSE_CLIENT -nm -q "
        system flush logs;
        select read_rows from system.query_log
            where
                event_date >= yesterday()
                and query_kind = 'Select' /* exclude DESC TABLE */
                and type = 'QueryFinish'
                and initial_query_id = '$query_id' and initial_query_id != query_id;
    " | xargs # convert new lines to spaces
}

function test_distributed_push_down_limit_0()
{
    local args=(
        "remote('127.{2,3}', $CLICKHOUSE_DATABASE, data_01814)"
        0 # offset
        --distributed_push_down_limit 0
    )
    test_distributed_push_down_limit_with_query_log "${args[@]}" "$@"
}

function test_distributed_push_down_limit_1()
{
    local args=(
        "remote('127.{2,3}', $CLICKHOUSE_DATABASE, data_01814, key)"
        0 # offset
        --distributed_push_down_limit 1
        --optimize_skip_unused_shards 1
        --optimize_distributed_group_by_sharding_key 1
    )
    test_distributed_push_down_limit_with_query_log "${args[@]}"
}

function test_distributed_push_down_limit_1_offset()
{
    local settings_and_opts=(
        --distributed_push_down_limit 1
        --optimize_skip_unused_shards 1
        --optimize_distributed_group_by_sharding_key 1
    )

    $CLICKHOUSE_CLIENT "${settings_and_opts[@]}" -q "select * from remote('127.{2,3}', $CLICKHOUSE_DATABASE, data_01814, key) group by key order by key desc limit 5, 10"
}

function main()
{
    setup
    trap cleanup EXIT

    echo 'distributed_push_down_limit=0'
    test_distributed_push_down_limit_0 --format Null

    #
    # The following tests (tests with distributed_push_down_limit=1) requires
    # retries, since the query may be canceled earlier due to LIMIT, and so
    # only one shard will be processed, and it will get not 40 but 20 rows:
    #
    #     1.160920 [ 291 ] {7ac5de70-c26c-4e3b-bdee-3873ad1b84f1} <Debug> executeQuery: (from [::ffff:127.0.0.1]:42778, initial_query_id: 66cf643c-b1b4-4f7e-942a-c4c3493029f6, using production parser) (comment: /usr/share/clickhouse-test/queries/0_stateless/01814_distributed_push_down_limit.sql) WITH CAST('test_31uut9', 'String') AS id_distributed_push_down_limit_1 SELECT key FROM test_31uut9.data_01814 GROUP BY key LIMIT 10
    #     1.214964 [ 291 ] {7ac5de70-c26c-4e3b-bdee-3873ad1b84f1} <Trace> ContextAccess (default): Access granted: SELECT(key) ON test_31uut9.data_01814
    #     1.216790 [ 291 ] {7ac5de70-c26c-4e3b-bdee-3873ad1b84f1} <Debug> test_31uut9.data_01814 (b484ad2e-0591-4faf-8110-1dcbd7cdd0db) (SelectExecutor): Key condition: unknown
    #     1.227245 [ 291 ] {7ac5de70-c26c-4e3b-bdee-3873ad1b84f1} <Debug> test_31uut9.data_01814 (b484ad2e-0591-4faf-8110-1dcbd7cdd0db) (SelectExecutor): Selected 1/1 parts by partition key, 1 parts by primary key, 10/11 marks by primary key, 10 marks to read from 1 ranges
    #     1.228452 [ 291 ] {7ac5de70-c26c-4e3b-bdee-3873ad1b84f1} <Trace> MergeTreeSelectProcessor: Reading 3 ranges from part all_1_1_0, approx. 100 rows starting from 0
    #     1.229104 [ 291 ] {7ac5de70-c26c-4e3b-bdee-3873ad1b84f1} <Trace> InterpreterSelectQuery: FetchColumns -> WithMergeableStateAfterAggregationAndLimit
    #     1.339085 [ 291 ] {7ac5de70-c26c-4e3b-bdee-3873ad1b84f1} <Information> TCPHandler: Query was cancelled.
    #     1.416573 [ 291 ] {7ac5de70-c26c-4e3b-bdee-3873ad1b84f1} <Information> executeQuery: Read 20 rows, 80.00 B in 0.254374666 sec., 78 rows/sec., 314.50 B/sec.
    #     1.419006 [ 291 ] {7ac5de70-c26c-4e3b-bdee-3873ad1b84f1} <Debug> MemoryTracker: Peak memory usage (for query): 0.00 B.
    #

    local out out_lines max_tries=20

    echo 'distributed_push_down_limit=1'
    for ((i = 0; i < max_tries; ++i)); do
        out=$(test_distributed_push_down_limit_1)
        out_lines=( $out )
        if [[ ${#out_lines[@]} -gt 2 ]] && [[ ${out_lines[-1]} = 40 ]] && [[ ${out_lines[-2]} = 40 ]]; then
            break
        fi
    done
    echo "$out"

    echo 'distributed_push_down_limit=1 with OFFSET'
    test_distributed_push_down_limit_1_offset
}
main "$@"
