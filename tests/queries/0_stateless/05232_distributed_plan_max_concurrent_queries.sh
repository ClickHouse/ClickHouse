#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the dispatched plan needs the stateless worker configuration (tests/config/config.d/distributed_query.xml).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A table's max_concurrent_queries counts queries, and one distributed-plan query reads the table
# through several fragments. The slot it takes must therefore stay taken until the last of those
# fragments stops reading, not until the first one finishes.

# The system.query_log rows the statements below are found through outlive their database, and a run
# with a fixed --database repeats $CLICKHOUSE_DATABASE, so an id must be one no other run can send.
run_id="${CLICKHOUSE_DATABASE}_${RANDOM}_$$"

# A fragment of the plan, identified by the initiator it is rooted at, that has started reading.
function wait_for_two_reading_fragments() {
    for _ in {1..600}; do
        if [[ $(${CLICKHOUSE_CLIENT} --query "SELECT countIf(read_rows > 0) FROM system.processes WHERE initial_query_id = '$1' AND query_id != '$1'") -ge 2 ]]; then
            return 0
        fi
        sleep 0.1
    done
    echo "no two fragments of $1 ever read the table, so the refusal below would not be attributable"
    return 1
}

for locally in 1 0; do
    table="t_dp_limit_$locally"
    ${CLICKHOUSE_CLIENT} --multiline --query "
    DROP TABLE IF EXISTS $table;

    CREATE TABLE $table (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1024, max_concurrent_queries = 1, min_marks_to_honor_max_concurrent_queries = 1;

    SYSTEM STOP MERGES $table;
    INSERT INTO $table SELECT number, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
    INSERT INTO $table SELECT number + 100000, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
    "

    echo "distributed_plan_execute_locally = $locally"
    query_id="05232_dp_limit_${locally}_$run_id"

    # Only the first 20000 keys sleep, and they are the leading marks of the first part, so the
    # fragment that owns them reads for ~20 s while its siblings finish at once. One granule per
    # block keeps a single sleep call near a second, so the KILL below lands promptly, and the
    # per-block sleep limit is raised because the number of rows in a block is not ours to pin.
    ${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "
        SELECT count() FROM $table WHERE k < 150000 AND (k >= 20000 OR NOT sleepEachRow(0.001))
        SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = $locally,
            enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0,
            distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 2,
            use_query_condition_cache = 0, max_block_size = 1024, max_execution_time = 300,
            function_sleep_max_microseconds_per_block = 60000000
        FORMAT Null" >/dev/null 2>&1 &

    wait_for_two_reading_fragments "$query_id" || exit 1

    # An independent reader of the same table. Parallel replicas are pinned off so that the refusal
    # can only come from the slot the plan above holds.
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM $table WHERE k < 150000
        SETTINGS make_distributed_plan = 0, enable_parallel_replicas = 0,
            automatic_parallel_replicas_mode = 0, use_query_condition_cache = 0
        FORMAT Null" 2>/dev/null
    CODE=$?
    [ "$CODE" -ne "202" ] && echo "Expected error code: 202 but got: $CODE" && exit 1
    if [[ $locally == 1 ]]; then
        echo "in-process plan: an unrelated reader is refused while the plan holds the table's only slot"
    else
        echo "dispatched plan: the slot outlives the fragment that took it, so an unrelated reader is refused"
    fi

    ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
    wait

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE $table"
done

# The sets of a statement read the table for the same statement, so they must share its slot rather than take one each:
# a set whose source cannot be shipped to the workers (`WITH TOTALS` here) is built on the initiator, and two sets used
# as values are each built through a distributed plan of their own, at the same time.
for locally in 1 0; do
    table="t_dp_set_$locally"
    ${CLICKHOUSE_CLIENT} --multiline --query "
    DROP TABLE IF EXISTS $table;

    CREATE TABLE $table (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1024, max_concurrent_queries = 1, min_marks_to_honor_max_concurrent_queries = 1;

    SYSTEM STOP MERGES $table;
    INSERT INTO $table SELECT number, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
    INSERT INTO $table SELECT number + 100000, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
    "

    settings="make_distributed_plan = 1, distributed_plan_execute_locally = $locally,
        enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0,
        distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 2,
        use_query_condition_cache = 0, max_block_size = 1024, function_sleep_max_microseconds_per_block = 60000000"

    echo "set built on the initiator, execute locally = $locally:"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), sum(k IN (SELECT k FROM $table WHERE k < 5000 GROUP BY k WITH TOTALS)) FROM $table
        SETTINGS $settings"

    # Each set source sleeps for ~3 s, so the two plans read the table at the same time.
    echo "two sets built through plans of their own, execute locally = $locally:"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT sum(k IN (SELECT k FROM $table WHERE k < 3000 AND NOT sleepEachRow(0.001))),
               sum(v IN (SELECT v FROM $table WHERE v < 3000 AND NOT sleepEachRow(0.001)))
        FROM $table
        SETTINGS $settings"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE $table"
done

# A client may send the statement itself as a secondary query, which carries no initial query id. The
# plan's fragments belong to it all the same, so they must share its slot rather than take one each.
for locally in 1 0; do
    table="t_dp_secondary_$locally"
    ${CLICKHOUSE_CLIENT} --multiline --query "
    DROP TABLE IF EXISTS $table;

    CREATE TABLE $table (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1024, max_concurrent_queries = 1, min_marks_to_honor_max_concurrent_queries = 1;

    SYSTEM STOP MERGES $table;
    INSERT INTO $table SELECT number, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
    INSERT INTO $table SELECT number + 100000, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
    "

    query_id="05232_dp_secondary_${locally}_$run_id"
    ${CLICKHOUSE_CLIENT} --query_kind secondary_query --query_id "$query_id" --query "
        SELECT count() FROM $table WHERE k < 150000
        SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = $locally,
            enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0,
            distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 2,
            use_query_condition_cache = 0
        FORMAT Null"
    CODE=$?
    [ "$CODE" -ne "0" ] && echo "Expected the secondary query to be served but got error code: $CODE" && exit 1

    # Being served says nothing on its own: a plan that collapsed to a single task would be served too.
    # The fragments are found through the id of the statement they belong to, which is that statement's
    # own id here, so this also asserts they no longer report an empty one.
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    fragments=$(${CLICKHOUSE_CLIENT} --query "
        SELECT countIf(ProfileEvents['SelectedParts'] > 0)
        FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 0
          AND initial_query_id = '$query_id' AND query_id != '$query_id'
        SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0")
    [ "$fragments" -lt 2 ] && echo "fewer than two fragments of the secondary query read the table" && exit 1

    echo "client-sent secondary query, execute locally = $locally: served through several fragments"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE $table"
done

# A refresh of a refreshable materialized view is a statement that does not go through `executeQuery`,
# and the fragments of its plan must share its slot all the same.
for locally in 1 0; do
    table="t_dp_refresh_$locally"
    view="mv_dp_refresh_${locally}_${RANDOM}_$$"
    ${CLICKHOUSE_CLIENT} --multiline --query "
    DROP TABLE IF EXISTS $table;
    DROP TABLE IF EXISTS ${table}_dst;

    CREATE TABLE $table (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1024, max_concurrent_queries = 1, min_marks_to_honor_max_concurrent_queries = 1;

    SYSTEM STOP MERGES $table;
    INSERT INTO $table SELECT number, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
    INSERT INTO $table SELECT number + 100000, number FROM numbers(100000) SETTINGS max_insert_threads = 1;

    CREATE TABLE ${table}_dst (c UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW $view REFRESH EVERY 1 YEAR SETTINGS refresh_retries = 0 APPEND TO ${table}_dst EMPTY AS
        SELECT count() AS c FROM $table WHERE k < 150000
        SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = $locally,
            enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0,
            distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 2,
            use_query_condition_cache = 0;

    SYSTEM REFRESH VIEW $view;
    SYSTEM WAIT VIEW $view;
    "

    echo "refresh, execute locally = $locally:"
    ${CLICKHOUSE_CLIENT} --query "SELECT c FROM ${table}_dst"

    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    fragments=$(${CLICKHOUSE_CLIENT} --query "
        SELECT countIf(ProfileEvents['SelectedParts'] > 0)
        FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 0
          AND log_comment = 'refresh of ${CLICKHOUSE_DATABASE}.$view'
        SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0")
    [ "$fragments" -lt 2 ] && echo "fewer than two fragments of the refresh read the table" && exit 1

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE $view"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}_dst"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE $table"
done
