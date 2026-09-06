#!/usr/bin/env bash
# Tags: no-fasttest, no-old-analyzer
# no-fasttest: the remote distributed plan needs the stateless worker configuration.
# no-old-analyzer: `make_distributed_plan` requires the analyzer.
# A satisfied LIMIT must stop the upstream stages of a distributed plan. The LIMIT is inside a
# subquery, so its stage is in the middle of the plan, not at the root. The query runs twice:
# with local in-memory exchanges and over the real streaming exchange transport. The remote run
# also depends on the streaming exchange sink flushing small chunks while its input is idle;
# without that the first rows never reach the LIMIT within the timeout.
#
# The assertion is the propagation itself, not the elapsed time. Every exchange endpoint traces
# the action it takes when the stop reaches it, and each stream between the LIMIT and the
# sleeping scan must show both halves: the sink closing its input and the source sending
# NoMoreDataNeeded upstream. A stream that merely ends by running out of data shows neither, so
# a normal completion cannot pass for an early stop. `max_execution_time` is only a backstop
# against a hang.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The probe scan sleeps 1s per 1000-row block. The first block satisfies the LIMIT; without the
# backward stop the scan runs on for 50+ seconds.
# Pinned: `max_block_size` and `index_granularity` keep `sleepEachRow` under its 3s per-block cap,
# `max_threads` keeps the full scan slower than the timeout, `join_algorithm` because a sorting
# join returns no rows until it reads all input, `min_joined_block_size_*` because squashing
# before the join would hold the first rows back until enough blocks accumulate,
# `max_rows_to_group_by` because the CI profile sets it and `make_distributed_plan` rejects
# an aggregation with a row limit, and the join order because a swap makes the probe table
# the build side, which also reads all input before the first row.
# The two bucket counts are pinned because the expected set of exchange streams below is derived
# from them.
COMMON_SETTINGS="make_distributed_plan = 1, enable_parallel_replicas = 0,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_force_exchange_kind = 'Streaming',
    max_block_size = 1000, max_threads = 2, join_algorithm = 'hash',
    query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false',
    min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0, max_rows_to_group_by = 0,
    max_execution_time = 300"

# Actions traced by an endpoint that received the stop while its own stage was idle. The receipt
# of the packet is deliberately not among them: it predates the propagation this test guards.
SINK_ACTIONS="'Closing input of exchange stream {}, no more data needed', 'Closing input of exchange stream {}, reader detached'"
SOURCE_ACTIONS="'NoMoreDataNeeded from exchange stream {}, total rows: {}, bytes: {}', 'NoMoreDataNeeded from exchange stream {}, detaching reader'"

# Streams between the LIMIT and the sleeping scan, as `EXPLAIN PLAN distributed = 1` lays them out:
# the shuffle that carries the scan into the join (3 reader buckets x 3 join buckets) and the
# gather directly below the LIMIT (3 join buckets x 1). The two dimension-table shuffles and
# everything downstream of the LIMIT are excluded: they run out of data instead of being stopped.
EXPECTED_STREAMS="arraySort(arrayConcat(
    arrayMap(i -> 'exchange_0__' || toString(intDiv(i, 3)) || '_' || toString(i % 3), range(9)),
    arrayMap(i -> 'exchange_2__' || toString(i) || '_0', range(3))))"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_dp_limit_stop (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000;
INSERT INTO t_dp_limit_stop SELECT number FROM numbers(300000);
CREATE TABLE t_dp_limit_stop_dim (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dp_limit_stop_dim SELECT number FROM numbers(1000);
"

function run_arm()
{
    local query_id="$1"
    local execute_locally="$2"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "
    SELECT count() FROM
    (
        SELECT l.x FROM t_dp_limit_stop AS l
        INNER JOIN t_dp_limit_stop_dim AS r ON l.x % 1000 = r.x
        WHERE NOT sleepEachRow(0.001)
        LIMIT 1
    )
    SETTINGS $COMMON_SETTINGS, distributed_plan_execute_locally = $execute_locally"
}

# Remote worker tasks run under their own `query_id` and are only reachable through
# `initial_query_id`, which `system.text_log` does not have. So resolve the query's own id from
# its coordinator row in this database, then take every task of that query and correlate the log
# rows by `query_id`. Worker tasks copy the global context, so their rows carry a different
# `current_database` and must not be filtered by it. Local tasks instead share the initiator's
# `query_id`, so the lookup below matches one row per task and has to aggregate to a single value.
function assert_stop_propagated()
{
    local label="$1"
    local query_id="$2"
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log, text_log"

    local root_query
    root_query=$($CLICKHOUSE_CLIENT --query "
        SELECT argMax(initial_query_id, event_time_microseconds) FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish'
          AND current_database = currentDatabase() AND query_id = '$query_id'")
    if [ -z "$root_query" ]; then
        echo "$label: no coordinator row for $query_id"
        return
    fi

    $CLICKHOUSE_CLIENT --query "
    WITH
        $EXPECTED_STREAMS AS expected,
        (
            SELECT arraySort(groupArray(stream)) FROM
            (
                SELECT value1 AS stream FROM system.text_log
                WHERE event_date >= yesterday()
                  AND query_id IN (
                      SELECT query_id FROM system.query_log
                      WHERE event_date >= yesterday() AND type = 'QueryFinish'
                        AND initial_query_id = '$root_query')
                  AND message_format_string IN ($SINK_ACTIONS, $SOURCE_ACTIONS)
                GROUP BY stream
                HAVING countIf(message_format_string IN ($SINK_ACTIONS)) > 0
                   AND countIf(message_format_string IN ($SOURCE_ACTIONS)) > 0
            )
        ) AS matched
    SELECT '$label: ' || if(matched = expected,
        'stop propagated on every expected exchange stream',
        'MISMATCH missing=' || toString(arrayFilter(s -> NOT has(matched, s), expected)) ||
        ' unexpected=' || toString(arrayFilter(s -> NOT has(expected, s), matched)))
    SETTINGS max_rows_to_read = 0"
}

run_arm "${CLICKHOUSE_TEST_UNIQUE_NAME}_local" 1
assert_stop_propagated "local exchanges" "${CLICKHOUSE_TEST_UNIQUE_NAME}_local"

run_arm "${CLICKHOUSE_TEST_UNIQUE_NAME}_remote" 0
assert_stop_propagated "remote exchanges" "${CLICKHOUSE_TEST_UNIQUE_NAME}_remote"

$CLICKHOUSE_CLIENT --query "
DROP TABLE t_dp_limit_stop;
DROP TABLE t_dp_limit_stop_dim;
"
