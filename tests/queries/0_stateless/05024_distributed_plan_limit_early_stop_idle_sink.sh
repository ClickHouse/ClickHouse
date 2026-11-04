#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the remote distributed plan needs the stateless worker configuration.
# A satisfied `LIMIT` must stop the upstream stages even when nothing flows through them
# anymore. Probe rows match the first join only in the first block, so after that block every
# stage upstream of the `LIMIT` goes silent. The backward stop must then cross idle exchanges:
# an idle `StreamingExchangeSink` must hear the `NoMoreDataNeeded` packet on its socket instead
# of with the next output chunk (which never comes), and an idle `StreamingExchangeSource` must
# notice that its output port was closed even though its peer sends no data, and forward
# `NoMoreDataNeeded` one hop upstream. If either half is missing, the sleeping scan runs on for
# 100+ seconds.
#
# The stop is checked in the tasks' `system.query_log` rows, not by elapsed time;
# `max_execution_time` is only a backstop against a hang.

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
# The bucket counts are pinned because the reference is derived from them, and
# `distributed_plan_fallback_to_local_execution` so a plan that cannot be distributed fails.
COMMON_SETTINGS="make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 0,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_force_exchange_kind = 'Streaming',
    max_block_size = 1000, max_threads = 2, join_algorithm = 'hash',
    query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false',
    min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0, max_rows_to_group_by = 0,
    max_execution_time = 300, distributed_plan_fallback_to_local_execution = 0"

PROBE_ROWS=300000

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_dp_idle_sink (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000;
"
# The matching rows must stay in the first block of the part; a parallel insert (randomized
# `max_insert_threads`/`max_threads` in CI) would scatter them to an arbitrary depth and the
# first probe block would no longer satisfy the `LIMIT`.
$CLICKHOUSE_CLIENT --query "
INSERT INTO t_dp_idle_sink SELECT if(number < 1000, number, number + 10000000) FROM numbers($PROBE_ROWS) SETTINGS max_threads = 1, max_insert_threads = 1;
CREATE TABLE t_dp_idle_sink_dim (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dp_idle_sink_dim SELECT number FROM numbers(1000);
"

# The lookups match by query id over the last 600 seconds of rows, so the id has to be unique per run.
# `CLICKHOUSE_TEST_UNIQUE_NAME` only varies with the database, and a run can be given a fixed
# database for a whole pass over the suite, which would let an earlier run of this test answer them.
QUERY_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_idle_sink_$(random_str 8)"

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

# A `text_log` flush waits for every line the server logged, minutes in a busy flaky check.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

# Remote task rows carry the worker's `current_database`, so only the initiator's row is filtered by it.
# The rows are counted, so all lookups read them locally rather than through parallel replicas.
INITIATOR_ROWS=$($CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND is_initial_query
      AND current_database = currentDatabase() AND query_id = '$QUERY_ID'
    SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0")

if [ "$INITIATOR_ROWS" != 1 ]; then
    echo "idle exchanges: expected one finished initiator row for $QUERY_ID, found $INITIATOR_ROWS"
else
    # A stage's sources stop only after its sink closed its input, so each count covers a whole hop:
    # `stage_4_*` needs the idle exchanges of the second join, `stage_2_*` the idle sinks of the first.
    # A join task also counts a dimension-table stream it is still reading when the stop comes, so 3 is a lower bound.
    $CLICKHOUSE_CLIENT --query "
    SELECT 'idle exchanges', 'StreamingExchangeEarlyCloses >= 3', query AS task, early_closes >= 3
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND NOT is_initial_query
      AND initial_query_id = '$QUERY_ID' AND (ProfileEvents['StreamingExchangeEarlyCloses'] AS early_closes) > 0
    ORDER BY task
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0"

    # The query waits for every stage, so if the stop never reaches the scan, the readers read all
    # `PROBE_ROWS` rows and one of the 3 reads a third or more.
    $CLICKHOUSE_CLIENT --query "
    SELECT 'idle exchanges', 'reader stopped early', query AS task, read_rows < $PROBE_ROWS / 3
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND NOT is_initial_query
      AND initial_query_id = '$QUERY_ID' AND startsWith(query, 'stage_0_')
    ORDER BY task
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0"
fi

$CLICKHOUSE_CLIENT --query "
DROP TABLE t_dp_idle_sink;
DROP TABLE t_dp_idle_sink_dim;
"
