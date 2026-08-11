#!/usr/bin/env bash
# Tags: long, no-replicated-database
# Tag no-replicated-database: DDL entries of a Replicated database execute in log order, so the
# concurrent CREATE checked by the test queues behind the waiting DROP by design.

# `DROP TABLE ... SYNC` of a table with inner tables (a materialized view, a `TimeSeries` table,
# a window view) waits for the inner tables to be finally dropped, which cannot happen while
# another query holds a reference to them. That wait must not run under the DDL guard of the
# table's name: a concurrent query can hold references to the inner tables and block on that
# DDL guard, which would deadlock with the waiting `DROP`. The test checks that while the `DROP`
# waits, DDL on the same table name still works - and that the `DROP` does wait.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function run_scenario()
{
    local label=$1
    local table_name=$2
    local inner_pattern=$3

    local inner_table
    inner_table=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '$inner_pattern' LIMIT 1")

    local holder_query_id="04839_holder_${label// /_}_${CLICKHOUSE_DATABASE}"

    # Holds a reference to the inner table for up to ~3 minutes (killed right after the checks
    # below). The right side of the join is its build side, so the query runs long while holding
    # a reference to the inner table even when the inner table is empty.
    $CLICKHOUSE_CLIENT --query_id "$holder_query_id" -q "SELECT count() FROM \`$inner_table\` AS a, (SELECT sleepEachRow(0.3) FROM numbers(600)) AS b SETTINGS max_block_size = 1, max_threads = 1, function_sleep_max_microseconds_per_block = 300000000, enable_parallel_replicas = 0, join_algorithm = 'hash', query_plan_join_swap_table = 'false' FORMAT Null" >/dev/null 2>&1 &
    local holder_pid=$!

    for _ in {1..300}
    do
        local started
        started=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$holder_query_id'")
        [[ $started == 1 ]] && break
        sleep 0.1
    done

    $CLICKHOUSE_CLIENT -q "DROP TABLE $table_name SYNC" &
    local drop_pid=$!

    # Wait until the DROP marks the inner tables as dropped: right after that it starts waiting
    # for them to be finally dropped, which cannot happen while the holder runs.
    for _ in {1..300}
    do
        local marked
        marked=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.dropped_tables WHERE database = currentDatabase() AND table = '$inner_table'")
        [[ $marked -ge 1 ]] && break
        sleep 0.1
    done

    # The waiting DROP must not hold the DDL guard of the name, so creating a new table
    # with the same name must succeed while the DROP is still waiting.
    if timeout 30 $CLICKHOUSE_CLIENT -q "CREATE TABLE $table_name (x UInt64) ENGINE = MergeTree ORDER BY x" >/dev/null 2>&1
    then
        echo "$label: concurrent CREATE with the same name succeeded while drop was waiting"
    else
        echo "$label: concurrent CREATE with the same name did not finish while drop was waiting"
    fi

    # The DROP must still be waiting for the pinned inner table (the wait is moved, not lost).
    if kill -0 $drop_pid 2>/dev/null
    then
        echo "$label: drop is still waiting for the pinned inner table"
    else
        echo "$label: drop finished without waiting for the pinned inner table"
    fi

    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$holder_query_id' ASYNC FORMAT Null"

    wait $drop_pid
    echo "$label: drop finished with code $?"
    wait $holder_pid

    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table_name SYNC"
}

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS mv_04839;
    DROP TABLE IF EXISTS src_04839;
    CREATE TABLE src_04839 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW mv_04839 ENGINE = MergeTree ORDER BY x AS SELECT x FROM src_04839;
"
run_scenario "materialized view" mv_04839 '.inner_id.%'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS src_04839 SYNC"

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 -q "CREATE TABLE ts_04839 ENGINE = TimeSeries"
run_scenario "time series" ts_04839 '.inner_id.samples.%'

$CLICKHOUSE_CLIENT --allow_experimental_window_view=1 --allow_experimental_analyzer=0 -q "
    CREATE TABLE wv_src_04839 (ts DateTime) ENGINE = MergeTree ORDER BY ts;
    CREATE WINDOW VIEW wv_04839 ENGINE = Memory WATERMARK toIntervalSecond(5) AS
        SELECT count() AS c, tumbleStart(w_id) AS w_start FROM wv_src_04839 GROUP BY tumble(ts, toIntervalSecond(1)) AS w_id;
"
run_scenario "window view" wv_04839 '.inner.%'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS wv_src_04839"
