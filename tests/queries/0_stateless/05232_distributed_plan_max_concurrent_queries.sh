#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the dispatched arm needs the stateless worker configuration (tests/config/config.d/distributed_query.xml).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A table's max_concurrent_queries counts queries, and one distributed-plan query reads the table
# through several fragments. The slot it takes must therefore stay taken until the last of those
# fragments stops reading, not until the first one finishes.

# The system.query_log rows the statements below are found through outlive their database, and a run
# with a fixed --database repeats $CLICKHOUSE_DATABASE, so an id must be one no other run can send.
run_id="${CLICKHOUSE_DATABASE}_${RANDOM}_$$"

# A fail point is server-global, so one left armed by an early exit fires in a test running alongside.
function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT parallel_replicas_wait_for_unused_replicas" 2>/dev/null
}
trap cleanup EXIT

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
table="t_pr_limit"
${CLICKHOUSE_CLIENT} --multiline --query "
DROP TABLE IF EXISTS $table;

CREATE TABLE $table (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1024, max_concurrent_queries = 1, min_marks_to_honor_max_concurrent_queries = 1;

SYSTEM STOP MERGES $table;
INSERT INTO $table SELECT number, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
INSERT INTO $table SELECT number + 100000, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
"

query_id="05232_pr_$run_id"
# Once every mark range is assigned the coordinator cancels the replicas that have not connected yet, and one
# request takes more marks than this table has, so the replica that announces first can be left reading alone.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas"
${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "
    SELECT count() FROM $table WHERE k < 150000
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
        parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0,
        automatic_parallel_replicas_mode = 0, use_query_condition_cache = 0, make_distributed_plan = 0
    FORMAT Null"
CODE=$?
# One-shot and server-global, so disarm it as soon as its statement returns, not at the end of the test.
cleanup
[ "$CODE" -ne "0" ] && echo "Expected the statement to be served but got error code: $CODE" && exit 1

# A replica takes the slot when it selects parts, before the coordinator hands out mark ranges, so a
# replica it later cancels held the slot all the same, and one the limit refuses reports its parts too.
# Each replica logs one terminal row, on a connection the statement above does not wait for, so a row can
# be queued after the statement's own flush: read the counts once every replica it read from has logged one.
TIMELIMIT=$((SECONDS + 30))
while [ $SECONDS -lt "$TIMELIMIT" ]; do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    read -r used replicas refused reported <<< "$(${CLICKHOUSE_CLIENT} --query "
        SELECT maxIf(ProfileEvents['ParallelReplicasAvailableCount'], is_initial_query),
               uniqExactIf(query_id, NOT is_initial_query AND exception_code != 202 AND ProfileEvents['SelectedParts'] > 0),
               countIf(NOT is_initial_query AND exception_code = 202 AND ProfileEvents['SelectedParts'] > 0),
               uniqExactIf(query_id, NOT is_initial_query AND type != 'QueryStart')
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND initial_query_id = '$query_id'
        SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0")"
    [ "$used" -gt 0 ] && [ "$reported" -ge "$used" ] && break
    sleep 0.2
done
${CLICKHOUSE_CLIENT} --query "DROP TABLE $table"
[ "$refused" -ne "0" ] && echo "the table's limit refused $refused replicas of the statement" && exit 1
[ "$used" -eq "0" ] && echo "the statement did not read the table through any replica" && exit 1
# A statement the coordinator served from a single replica has no shared slot to show.
[ "$used" -ge 2 ] && [ "$replicas" -lt 2 ] && echo "fewer than two replicas of the statement took the table's slot: $replicas" && exit 1

# A replica's read carries the default database rather than this test's, which is why the rows above are
# found through the initiator's id; this anchors that id to a statement this test ran.
${CLICKHOUSE_CLIENT} --query "
    SELECT throwIf(count() = 0, 'the rows counted above do not belong to a statement of this test')
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
      AND current_database = currentDatabase() AND query_id = '$query_id'
    SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0
    FORMAT Null"
echo "one statement holding the table's only slot on several replicas is served"

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
