#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the dispatched arm needs the stateless worker configuration (tests/config/config.d/distributed_query.xml).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A table's max_concurrent_queries counts queries, and one distributed-plan query reads the table
# through several fragments. The slot it takes must therefore stay taken until the last of those
# fragments stops reading, not until the first one finishes.

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
    query_id="05232_dp_limit_${locally}_$CLICKHOUSE_DATABASE"

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
    # Measured: only the dispatched arm discriminates the release on the last reader, because the
    # in-process fragments never exit early, they stop together with the statement that owns them.
    if [[ $locally == 1 ]]; then
        echo "in-process plan: an unrelated reader is refused while the plan holds the table's only slot"
    else
        echo "dispatched plan: the slot outlives the fragment that took it, so an unrelated reader is refused"
    fi

    ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
    wait

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE $table"
done

# Parallel replicas read the table the same way, on several replicas of this server at once, and they
# are one query too, so the statement must not reject itself against a limit of one.
echo "parallel replicas"
# How many replicas answer is not ours to pin (measured: 27 of 30 statements read the table on more
# than one), and a statement served by a single replica would say nothing about sharing a slot. So
# every attempt must be served, and the assertion is made on the first one that really did fan out.
# Each attempt reads its own table, so that no attempt can be refused by its predecessor's slot.
fanout=0
for attempt in {1..10}; do
    table="t_pr_limit_$attempt"
    ${CLICKHOUSE_CLIENT} --multiline --query "
    DROP TABLE IF EXISTS $table;

    CREATE TABLE $table (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1024, max_concurrent_queries = 1, min_marks_to_honor_max_concurrent_queries = 1;

    SYSTEM STOP MERGES $table;
    INSERT INTO $table SELECT number, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
    INSERT INTO $table SELECT number + 100000, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
    "

    query_id="05232_pr_${attempt}_$CLICKHOUSE_DATABASE"
    ${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "
        SELECT count() FROM $table WHERE k < 150000
        SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
            cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
            parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0,
            automatic_parallel_replicas_mode = 0, use_query_condition_cache = 0, make_distributed_plan = 0
        FORMAT Null"
    CODE=$?
    [ "$CODE" -ne "0" ] && echo "Expected the statement to be served but got error code: $CODE" && exit 1

    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    fanout=$(${CLICKHOUSE_CLIENT} --query "
        SELECT countIf(ProfileEvents['SelectedParts'] > 0)
        FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 0
          AND initial_query_id = '$query_id'
        SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0")
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE $table"
    [ "$fanout" -ge 2 ] && break
done
[ "$fanout" -lt 2 ] && echo "no attempt read the table on more than one replica" && exit 1

# A replica's read carries the default database rather than this test's, which is why the rows above are
# found through the initiator's id; this anchors that id to a statement this test ran.
${CLICKHOUSE_CLIENT} --query "
    SELECT throwIf(count() = 0, 'the rows counted above do not belong to a statement of this test')
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish'
      AND current_database = currentDatabase() AND query_id = '$query_id'
    SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0
    FORMAT Null"
echo "one statement reading the table on several replicas is served"
