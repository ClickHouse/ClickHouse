#!/usr/bin/env bash
# Tags: no-parallel-replicas, long
# Checks write accounting (query_log, asynchronous_insert_log, X-ClickHouse-Summary, quotas) for
# INSERT ... SELECT routed through the async insert queue, versus the synchronous route.
# no-parallel-replicas: cases key off query_id and quota name, a concurrent run could shift them.
# long: many cases poll system log tables sequentially, this exceeds the flaky check's 180s cap.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin threads: a randomized thread count could split the 3-row result into more than one block.
PINNED_SETTINGS=(
    "max_threads=1"
    "max_insert_threads=1"
)
PINNED_SETTINGS_SQL=$(IFS=,; echo "${PINNED_SETTINGS[*]}")
PINNED_SETTINGS_URL=$(IFS='&'; echo "${PINNED_SETTINGS[*]}")

# Write-side accounting for the queue route: `InterpreterInsertQuery::addInsertToSelectPipeline`
# plus `AsyncInsertQueueTransform`. The diverted block is written by the flush, not by this
# query's own pipeline, so it must be counted exactly once, by the flush, only on success.
# Counting it here too would double the write in query_log, X-ClickHouse-Summary and the
# WRITTEN_BYTES quota, and would charge writes a failing flush never performed.

# A log element is queued for writing after the query returns, so one FLUSH LOGS right after can
# miss it: https://github.com/ClickHouse/ClickHouse/issues/84364. Retry until the rows appear or
# the bounded loop times out. Result is left in LOG_ROW_COUNT for callers that just need the count.
wait_for_log_rows()
{
    local log_table=$1 && shift
    local expected=$1 && shift
    local count_query=$1 && shift

    LOG_ROW_COUNT=0
    for _ in $(seq 1 60); do
        ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS ${log_table}"
        LOG_ROW_COUNT=$(${CLICKHOUSE_CLIENT} -q "${count_query}")
        [ "$LOG_ROW_COUNT" -ge "$expected" ] && return
        sleep 0.5
    done
    echo "timed out waiting for ${expected} rows in system.${log_table}, got ${LOG_ROW_COUNT}"
}

# Case 1: the routed insert and the same insert on the synchronous route report the same write.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04633_acc"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04633_acc (n UInt64) ENGINE = MergeTree ORDER BY n"

QUEUED_ID="test_04633_queued_$RANDOM"
SYNC_ID="test_04633_sync_$RANDOM"
${CLICKHOUSE_CLIENT} --query_id="$QUEUED_ID" -q "
    INSERT INTO test_04633_acc SELECT number FROM numbers(3)
    SETTINGS async_insert = 1, wait_for_async_insert = 1, $PINNED_SETTINGS_SQL
"
${CLICKHOUSE_CLIENT} --query_id="$SYNC_ID" -q "
    INSERT INTO test_04633_acc SELECT number FROM numbers(3)
    SETTINGS async_insert = 0, $PINNED_SETTINGS_SQL
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04633_acc"

# Only the queued insert reached the queue, so the numbers below compare the two routes fairly.
wait_for_log_rows asynchronous_insert_log 1 "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase() AND table = 'test_04633_acc'
"
echo "$LOG_ROW_COUNT"
wait_for_log_rows query_log 2 "
    SELECT count()
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase() AND type = 'QueryFinish'
      AND query_id IN ('$QUEUED_ID', '$SYNC_ID')
"
# written_rows: 3, not 6, on both routes. Bytes must match between routes too.
${CLICKHOUSE_CLIENT} -q "
    SELECT
        anyIf(written_rows, query_id = '$QUEUED_ID'),
        anyIf(written_rows, query_id = '$SYNC_ID'),
        anyIf(written_bytes, query_id = '$QUEUED_ID') = anyIf(written_bytes, query_id = '$SYNC_ID')
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase() AND type = 'QueryFinish'
      AND query_id IN ('$QUEUED_ID', '$SYNC_ID')
"
# The routed insert's write-side ProfileEvents belong to the flush query, not this one.
${CLICKHOUSE_CLIENT} -q "
    SELECT
        anyIf(ProfileEvents['InsertedRows'], query_id = '$QUEUED_ID'),
        anyIf(ProfileEvents['InsertedRows'], query_id = '$SYNC_ID')
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase() AND type = 'QueryFinish'
      AND query_id IN ('$QUEUED_ID', '$SYNC_ID')
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04633_acc"

# Case 2: the same numbers as seen by an HTTP client in X-ClickHouse-Summary.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04633_summary"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04633_summary (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CURL} -sS -v "${CLICKHOUSE_URL}&http_wait_end_of_query=1&async_insert=1&wait_for_async_insert=1&$PINNED_SETTINGS_URL" \
    -d "INSERT INTO test_04633_summary SELECT number FROM numbers(3)" 2>&1 \
    | grep -o '"written_rows":"[0-9]*","written_bytes":"[0-9]*"'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04633_summary"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04633_summary"

# Case 3: a failing flush must report no write. The CHECK constraint fires in the flush's own
# sink chain, so the exception reaches the client via the future, after the block was queued.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04633_failing"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_04633_failing (n UInt64, CONSTRAINT c_small CHECK n < 100)
    ENGINE = MergeTree ORDER BY n
"
FAILED_ID="test_04633_failed_$RANDOM"
${CLICKHOUSE_CLIENT} --query_id="$FAILED_ID" -q "
    INSERT INTO test_04633_failing SELECT 1000 + number FROM numbers(3)
    SETTINGS async_insert = 1, wait_for_async_insert = 1, $PINNED_SETTINGS_SQL
" 2>&1 | grep -m1 -o VIOLATED_CONSTRAINT
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04633_failing"

wait_for_log_rows asynchronous_insert_log 1 "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase() AND table = 'test_04633_failing'
"
wait_for_log_rows query_log 1 "
    SELECT count()
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase() AND type = 'ExceptionWhileProcessing'
      AND query_id = '$FAILED_ID'
"
${CLICKHOUSE_CLIENT} -q "
    SELECT status
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase() AND table = 'test_04633_failing'
"
${CLICKHOUSE_CLIENT} -q "
    SELECT written_rows, written_bytes
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase() AND type = 'ExceptionWhileProcessing'
      AND query_id = '$FAILED_ID'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04633_failing"

# Case 4: WRITTEN_BYTES quota is charged once per route: by the flush, on the queue route.
ROLE="r_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
QUOTA="q_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04633_quota"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04633_quota (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "CREATE ROLE ${ROLE}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER}"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON *.* TO ${ROLE}"
${CLICKHOUSE_CLIENT} -q "GRANT ${ROLE} TO ${USER}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX WRITTEN BYTES = 1000000 TO ${ROLE}"

${CLICKHOUSE_CLIENT} --user "${USER}" -q "
    INSERT INTO ${CLICKHOUSE_DATABASE}.test_04633_quota SELECT number FROM numbers(3)
    SETTINGS async_insert = 1, wait_for_async_insert = 1, $PINNED_SETTINGS_SQL
"
QUEUED_BYTES=$(${CLICKHOUSE_CLIENT} -q "SELECT sum(written_bytes) FROM system.quotas_usage WHERE quota_name = '${QUOTA}'")
${CLICKHOUSE_CLIENT} --user "${USER}" -q "
    INSERT INTO ${CLICKHOUSE_DATABASE}.test_04633_quota SELECT number FROM numbers(3)
    SETTINGS async_insert = 0, $PINNED_SETTINGS_SQL
"
TOTAL_BYTES=$(${CLICKHOUSE_CLIENT} -q "SELECT sum(written_bytes) FROM system.quotas_usage WHERE quota_name = '${QUOTA}'")

wait_for_log_rows asynchronous_insert_log 1 "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase() AND table = 'test_04633_quota'
"
echo "$LOG_ROW_COUNT"
# The routed insert charged something, and it charged exactly as much as the synchronous one.
if [[ "$QUEUED_BYTES" -gt 0 && $((TOTAL_BYTES - QUEUED_BYTES)) -eq "$QUEUED_BYTES" ]]; then
    echo "quota charged once per route"
else
    echo "quota mismatch: queued=$QUEUED_BYTES total=$TOTAL_BYTES"
fi

${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "DROP USER ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE ${ROLE}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04633_quota"

# Case 5: `max_execution_time` with `timeout_overflow_mode = 'break'` must not cut the wait for
# the flush short. The busy timeout is pinned well above max_execution_time (adaptive busy timeout
# off), so the limit elapses while the query still waits; breaking out there would report success
# for a flush that never ran.
#
# The flush itself gets a fresh max_execution_time and may or may not commit in time under
# 'break'. That is not asserted here, since it depends on machine speed.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04633_break"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04633_break (n UInt64) ENGINE = MergeTree ORDER BY n"
BREAK_ID="test_04633_break_$RANDOM"
${CLICKHOUSE_CLIENT} --query_id="$BREAK_ID" -q "
    INSERT INTO test_04633_break SELECT number FROM numbers(3)
    SETTINGS async_insert = 1, wait_for_async_insert = 1,
             async_insert_use_adaptive_busy_timeout = 0,
             async_insert_busy_timeout_min_ms = 2000, async_insert_busy_timeout_max_ms = 2000,
             max_execution_time = 1, timeout_overflow_mode = 'break',
             $PINNED_SETTINGS_SQL
"
wait_for_log_rows query_log 1 "
    SELECT count()
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase() AND type = 'QueryFinish'
      AND query_id = '$BREAK_ID'
"
# Lower bound only, so a slow machine can't fail this: breaking out at 1000 ms ends the query
# around there, while waiting for the flush can't end it before the 2000 ms busy timeout.
${CLICKHOUSE_CLIENT} -q "
    SELECT if(query_duration_ms >= 1500,
              'waited for the flush',
              concat('returned early after ', toString(query_duration_ms), ' ms'))
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase() AND type = 'QueryFinish'
      AND query_id = '$BREAK_ID'
"

wait_for_log_rows asynchronous_insert_log 1 "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase() AND table = 'test_04633_break'
"
echo "$LOG_ROW_COUNT"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04633_break"

# Case 6: `KILL QUERY` while waiting for the queued block to flush must surface as cancelled, not
# a silent success. The wait loop must check `throwIfKilled()`, not just `isCancelled()`, since
# `QueryStatus::cancelQuery` marks the query killed before it cancels the pipeline executors.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04633_kill"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04633_kill (n UInt64) ENGINE = MergeTree ORDER BY n"

KILL_ID="test_04633_kill_$RANDOM"
KILL_OUT=$(mktemp)
${CLICKHOUSE_CLIENT} --query_id="$KILL_ID" -q "
    INSERT INTO test_04633_kill SELECT number FROM numbers(3)
    SETTINGS async_insert = 1, wait_for_async_insert = 1,
             async_insert_use_adaptive_busy_timeout = 0,
             async_insert_busy_timeout_min_ms = 30000, async_insert_busy_timeout_max_ms = 30000,
             $PINNED_SETTINGS_SQL
" > "$KILL_OUT" 2>&1 &
INSERT_PID=$!
wait_for_query_to_start "$KILL_ID" 30
${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$KILL_ID' SYNC FORMAT Null"
wait "$INSERT_PID"
INSERT_EXIT=$?
[ "$INSERT_EXIT" -ne 0 ] && echo "client saw the insert fail" || echo "client saw the insert succeed"
grep -q QUERY_WAS_CANCELLED "$KILL_OUT" && echo "client error mentions QUERY_WAS_CANCELLED" || echo "client error did not mention QUERY_WAS_CANCELLED"
rm -f "$KILL_OUT"

wait_for_log_rows query_log 1 "
    SELECT count()
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase() AND type = 'ExceptionWhileProcessing'
      AND query_id = '$KILL_ID'
"
${CLICKHOUSE_CLIENT} -q "
    SELECT exception_code
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase() AND type = 'ExceptionWhileProcessing'
      AND query_id = '$KILL_ID'
"

# The block was queued before the kill landed, so the queue still flushes it in the background:
# cancelling stops the wait, not the write. Drain it before dropping the table.
wait_for_log_rows asynchronous_insert_log 1 "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase() AND table = 'test_04633_kill'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04633_kill"

# Case 7: `max_execution_time` expires in the same poll window as the flush completes. A ready
# flush does not undo an already-expired limit, so the query must still fail.
#
# The flush completes about 2000 ms after queuing, and the 1.99 s limit expires just before that.
# A slower start moves the limit to an earlier poll, the path case 5 already covers.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04633_ready_race"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04633_ready_race (n UInt64) ENGINE = MergeTree ORDER BY n"

RACE_ID="test_04633_race_$RANDOM"
${CLICKHOUSE_CLIENT} --query_id="$RACE_ID" -q "
    INSERT INTO test_04633_ready_race SELECT number FROM numbers(3)
    SETTINGS async_insert = 1, wait_for_async_insert = 1,
             async_insert_use_adaptive_busy_timeout = 0,
             async_insert_busy_timeout_min_ms = 2000, async_insert_busy_timeout_max_ms = 2000,
             max_execution_time = 1.99, timeout_overflow_mode = 'throw',
             $PINNED_SETTINGS_SQL
" 2>&1 | grep -m1 -o TIMEOUT_EXCEEDED

wait_for_log_rows query_log 1 "
    SELECT count()
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase() AND type = 'ExceptionWhileProcessing'
      AND query_id = '$RACE_ID'
"
echo "$LOG_ROW_COUNT"

# The block was queued before the limit expired, so the flush may still commit it: not asserted,
# drained only to keep the queue off a dropped table.
wait_for_log_rows asynchronous_insert_log 1 "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase() AND table = 'test_04633_ready_race'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04633_ready_race"
