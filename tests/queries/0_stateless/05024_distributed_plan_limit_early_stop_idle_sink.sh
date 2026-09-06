#!/usr/bin/env bash
# Tags: no-fasttest, no-old-analyzer
# no-fasttest: the remote distributed plan needs the stateless worker configuration.
# no-old-analyzer: `make_distributed_plan` requires the analyzer.
# A satisfied `LIMIT` must stop the upstream stages even when nothing flows through them
# anymore. Probe rows match the first join only in the first block, so after that block every
# stage upstream of the `LIMIT` goes silent. The backward stop must then cross idle exchanges:
# an idle `StreamingExchangeSink` must hear the `NoMoreDataNeeded` packet on its socket instead
# of with the next output chunk (which never comes), and an idle `StreamingExchangeSource` must
# notice that its output port was closed even though its peer sends no data, and forward
# `NoMoreDataNeeded` one hop upstream. If either half is missing, the sleeping scan runs on for
# 100+ seconds.
#
# The assertion is the propagation itself, not the elapsed time. Every exchange endpoint traces
# the action it takes when the stop reaches it, and each stream between the `LIMIT` and the
# sleeping scan must show both halves: the sink closing its input and the source sending
# `NoMoreDataNeeded` upstream. A stream that merely ends by running out of data shows neither, so
# a normal completion cannot pass for an early stop. `max_execution_time` is only a backstop
# against a hang.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The probe scan sleeps 1s per 1000-row block. The first block satisfies the `LIMIT` through both
# joins; rows of later blocks are shifted by 10000000, so the first join matches nothing and its
# stage never touches its sink again. The two joins use different keys, so an exchange separates
# their stages and the stop signal has to cross it backward.
# Pinned: `max_block_size` and `index_granularity` keep `sleepEachRow` under its 3s per-block cap,
# `max_threads` keeps the full scan slower than the timeout, `join_algorithm` because a sorting
# join returns no rows until it reads all input, `min_joined_block_size_*` because squashing
# before the join would hold the first rows back until enough blocks accumulate,
# `max_rows_to_group_by` because the CI profile sets it and `make_distributed_plan` rejects
# an aggregation with a row limit, and the join order because a swap makes the probe table
# the build side, which also reads all input before the first row.
# The two bucket counts are pinned because the expected set of exchange streams below is derived
# from them.
COMMON_SETTINGS="make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 0,
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

# Streams between the `LIMIT` and the sleeping scan, as `EXPLAIN PLAN distributed = 1` lays them
# out: the shuffle that carries the scan into the first join, the shuffle between the two
# differently-keyed joins (the idle one this test exists for), each 3 source buckets x 3
# destination buckets, and the gather directly below the `LIMIT`, 3 join buckets x 1. The two
# dimension-table shuffles and everything downstream of the `LIMIT` are excluded: they run out of
# data instead of being stopped.
EXPECTED_STREAMS="arraySort(arrayConcat(
    arrayMap(i -> 'exchange_0__' || toString(intDiv(i, 3)) || '_' || toString(i % 3), range(9)),
    arrayMap(i -> 'exchange_2__' || toString(intDiv(i, 3)) || '_' || toString(i % 3), range(9)),
    arrayMap(i -> 'exchange_4__' || toString(i) || '_0', range(3))))"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_dp_idle_sink (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000;
"
# The matching rows must stay in the first block of the part; a parallel insert (randomized
# `max_insert_threads`/`max_threads` in CI) would scatter them to an arbitrary depth and the
# first probe block would no longer satisfy the `LIMIT`.
$CLICKHOUSE_CLIENT --query "
INSERT INTO t_dp_idle_sink SELECT if(number < 1000, number, number + 10000000) FROM numbers(300000) SETTINGS max_threads = 1, max_insert_threads = 1;
CREATE TABLE t_dp_idle_sink_dim (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dp_idle_sink_dim SELECT number FROM numbers(1000);
"

QUERY_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_idle_sink"

$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --query "
SELECT count() FROM
(
    SELECT s.x FROM
    (
        SELECT l.x FROM t_dp_idle_sink AS l
        INNER JOIN t_dp_idle_sink_dim AS r ON l.x = r.x
        WHERE NOT sleepEachRow(0.001)
    ) AS s
    INNER JOIN t_dp_idle_sink_dim AS r2 ON s.x % 1000 = r2.x
    LIMIT 1
)
SETTINGS $COMMON_SETTINGS"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log, text_log"

# Remote worker tasks run under their own `query_id` and are only reachable through
# `initial_query_id`, which `system.text_log` does not have. So resolve the query's own id from
# its coordinator row in this database, then take every task of that query and correlate the log
# rows by `query_id`. Worker tasks copy the global context, so their rows carry a different
# `current_database` and must not be filtered by it.
ROOT_QUERY=$($CLICKHOUSE_CLIENT --query "
    SELECT argMax(initial_query_id, event_time_microseconds) FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish'
      AND current_database = currentDatabase() AND query_id = '$QUERY_ID'")

if [ -z "$ROOT_QUERY" ]; then
    echo "idle exchanges: no coordinator row for $QUERY_ID"
else
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
                        AND initial_query_id = '$ROOT_QUERY')
                  AND message_format_string IN ($SINK_ACTIONS, $SOURCE_ACTIONS)
                GROUP BY stream
                HAVING countIf(message_format_string IN ($SINK_ACTIONS)) > 0
                   AND countIf(message_format_string IN ($SOURCE_ACTIONS)) > 0
            )
        ) AS matched
    SELECT 'idle exchanges: ' || if(matched = expected,
        'stop propagated on every expected exchange stream',
        'MISMATCH missing=' || toString(arrayFilter(s -> NOT has(matched, s), expected)) ||
        ' unexpected=' || toString(arrayFilter(s -> NOT has(expected, s), matched)))
    SETTINGS max_rows_to_read = 0"
fi

$CLICKHOUSE_CLIENT --query "
DROP TABLE t_dp_idle_sink;
DROP TABLE t_dp_idle_sink_dim;
"
