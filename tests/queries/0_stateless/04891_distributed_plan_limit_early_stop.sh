#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the remote distributed plan needs the stateless worker configuration.
# A satisfied LIMIT must stop the upstream stages of a distributed plan. The LIMIT is inside a
# subquery, so its stage is in the middle of the plan, not at the root. The query runs twice:
# with local in-memory exchanges and over the real streaming exchange transport. The remote run
# also depends on the streaming exchange sink flushing small chunks while its input is idle;
# without that the first rows never reach the LIMIT within the timeout.
#
# The stop is checked in the tasks' `system.query_log` rows, not by elapsed time;
# `max_execution_time` is only a backstop against a hang.

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
# The bucket counts are pinned because the reference is derived from them, and
# `distributed_plan_fallback_to_local_execution` so a plan that cannot be distributed fails.
COMMON_SETTINGS="make_distributed_plan = 1, enable_parallel_replicas = 0,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_force_exchange_kind = 'Streaming',
    max_block_size = 1000, max_threads = 2, join_algorithm = 'hash',
    query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false',
    min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0, max_rows_to_group_by = 0,
    max_execution_time = 300, distributed_plan_fallback_to_local_execution = 0"

PROBE_ROWS=300000

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_dp_limit_stop (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000;
INSERT INTO t_dp_limit_stop SELECT number FROM numbers($PROBE_ROWS);
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

# Prints the stop counts and the reader checks from the `system.query_log` rows of the query's tasks.
function assert_stop_propagated()
{
    local label="$1"
    local query_id="$2"
    # A `text_log` flush waits for every line the server logged, minutes in a busy flaky check.
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

    # Remote task rows carry the worker's `current_database`, so only the initiator's row is filtered by it.
    # The rows are counted, so all lookups read them locally rather than through parallel replicas.
    local initiator_rows
    initiator_rows=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query
          AND current_database = currentDatabase() AND query_id = '$query_id'
        SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0")
    if [ "$initiator_rows" != 1 ]; then
        echo "$label: expected one finished initiator row for $query_id, found $initiator_rows"
        return
    fi

    # A stage's sources stop only after its sink closed its input, so each count covers a whole hop.
    # In-memory exchanges count nothing.
    $CLICKHOUSE_CLIENT --query "
    SELECT '$label', 'StreamingExchangeEarlyCloses', query AS task, ProfileEvents['StreamingExchangeEarlyCloses'] AS early_closes
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query
      AND initial_query_id = '$query_id' AND early_closes > 0
    ORDER BY task
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0"

    # The query waits for every stage, so if the stop never reaches the scan, the readers read all
    # `PROBE_ROWS` rows and one of the 3 reads a third or more.
    $CLICKHOUSE_CLIENT --query "
    SELECT '$label', 'reader stopped early', query AS task, read_rows < $PROBE_ROWS / 3
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query
      AND initial_query_id = '$query_id' AND startsWith(query, 'stage_0_')
    ORDER BY task
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0"
}

# The lookups match by query id over a full day of rows, so the id has to be unique per run.
# `CLICKHOUSE_TEST_UNIQUE_NAME` only varies with the database, and a run can be given a fixed
# database for a whole pass over the suite, which would let an earlier run of this test answer them.
RUN_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_$(random_str 8)"

run_arm "${RUN_ID}_local" 1
assert_stop_propagated "local exchanges" "${RUN_ID}_local"

run_arm "${RUN_ID}_remote" 0
assert_stop_propagated "remote exchanges" "${RUN_ID}_remote"

$CLICKHOUSE_CLIENT --query "
DROP TABLE t_dp_limit_stop;
DROP TABLE t_dp_limit_stop_dim;
"
