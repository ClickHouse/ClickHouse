#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

DB="${CLICKHOUSE_DATABASE}_write_cancel"
CLIENT_PID=""
CLIENT_ERR="${CLICKHOUSE_TMP}/write_first_cancel.err"

cleanup()
{
    if [[ -n "$CLIENT_PID" ]]; then
        kill -KILL "$CLIENT_PID" 2>/dev/null || true
        wait "$CLIENT_PID" 2>/dev/null || true
    fi
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $DB SYNC"
    rm -f "$CLIENT_ERR"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "CREATE DATABASE $DB ENGINE=Atomic"
$CLICKHOUSE_CLIENT --multiquery --query "
    CREATE TABLE $DB.source (n UInt64) ENGINE=MergeTree ORDER BY n
        SETTINGS index_granularity=1000, index_granularity_bytes=0,
        min_rows_for_wide_part=0, min_bytes_for_wide_part=0;
    INSERT INTO $DB.source SELECT number FROM numbers(1000000);
    CREATE TABLE $DB.dst (n UInt64) ENGINE=MergeTree ORDER BY n;
    CREATE TABLE $DB.dst2 (n UInt64) ENGINE=MergeTree ORDER BY n;
    CREATE TABLE $DB.replaced (n UInt64) ENGINE=MergeTree ORDER BY n;
    INSERT INTO $DB.replaced VALUES (7777777);"

# Keep all generated rows below the insert squashing threshold. Cancellation must
# discard these buffered rows; this does not assert rollback of committed writes.
SLOW="SELECT number AS n FROM numbers(1000000) WHERE sleepEachRow(0.0001)=0"

run_cancelled_query()
{
    local name="$1" query="$2" analyzer="$3" expected_exception_code="${4:-735}"
    local query_id="${DB}_${name}_${analyzer}"
    local ready=0

    $CLICKHOUSE_CLIENT --query_id "$query_id" \
        --partial_result_on_first_cancel=1 --enable_analyzer="$analyzer" \
        --max_threads=1 --max_insert_threads=1 --max_block_size=1000 --preferred_block_size_bytes=0 \
        --min_insert_block_size_rows=100000000 --min_insert_block_size_bytes=1000000000 \
        --interactive_delay=1000 --max_execution_time=0 --log_queries=1 \
        --log_queries_probability=1 --log_queries_min_query_duration_ms=0 \
        --use_query_cache=0 --use_index_for_in_with_subqueries=1 --enable_parallel_replicas=0 \
        --query "$query" > /dev/null 2> "$CLIENT_ERR" &
    CLIENT_PID=$!

    # `PARALLEL WITH` does not aggregate the operands' read counters into the root.
    # Its million-row sleeping sources cannot finish during this bounded wait.
    for _ in {1..200}; do
        if [[ "$($CLICKHOUSE_CLIENT --query "
            SELECT count() FROM system.processes WHERE query_id='$query_id'
            AND (read_rows >= 1000 OR ('$name' = 'parallel' AND elapsed > 1))")" == 1 ]]; then
            ready=1
            break
        fi
        if ! kill -0 "$CLIENT_PID" 2>/dev/null; then
            break
        fi
        sleep 0.1
    done
    if [[ "$ready" != 1 ]]; then
        echo "Query did not reach cancellation boundary: $name / $analyzer"
        cat "$CLIENT_ERR"
        return 1
    fi

    kill -INT "$CLIENT_PID"
    wait "$CLIENT_PID" || true
    CLIENT_PID=""

    # The client can swallow the exception after its own `SIGINT`. Check the
    # server's terminal record, including cancellation during query analysis.
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    local cancelled
    cancelled=$($CLICKHOUSE_CLIENT --query "
        SELECT count() = 1 AND countIf(exception_code = $expected_exception_code
            AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')) = 1
        FROM system.query_log WHERE current_database = currentDatabase()
            AND query_id='$query_id' AND type != 'QueryStart'")
    if [[ "$cancelled" != 1 ]]; then
        echo "Query did not report full cancellation: $name / $analyzer"
        cat "$CLIENT_ERR"
        return 1
    fi
    echo "$name / $analyzer: cancelled"
}

run_cancelled_query insert "INSERT INTO $DB.dst $SLOW" 1
$CLICKHOUSE_CLIENT --query "SELECT count() FROM $DB.dst"
run_cancelled_query create "CREATE TABLE $DB.created ENGINE=MergeTree ORDER BY n AS $SLOW" 1
$CLICKHOUSE_CLIENT --query "EXISTS TABLE $DB.created"
run_cancelled_query replace "CREATE OR REPLACE TABLE $DB.replaced ENGINE=MergeTree ORDER BY n AS $SLOW" 1
$CLICKHOUSE_CLIENT --query "SELECT n FROM $DB.replaced"
run_cancelled_query populate "CREATE MATERIALIZED VIEW $DB.mv ENGINE=MergeTree ORDER BY n POPULATE AS
    SELECT n FROM $DB.source WHERE sleepEachRow(0.0001)=0 SETTINGS materialized_views_populate_atomically=1, max_block_size=1000, preferred_block_size_bytes=0" 1
$CLICKHOUSE_CLIENT --query "EXISTS TABLE $DB.mv"
run_cancelled_query parallel "INSERT INTO $DB.dst $SLOW PARALLEL WITH INSERT INTO $DB.dst2 $SLOW" 1
$CLICKHOUSE_CLIENT --query "SELECT (SELECT count() FROM $DB.dst), (SELECT count() FROM $DB.dst2)"

for analyzer in 0 1; do
    # Scalar evaluation and primary-key set construction can consume the first
    # `Cancel` before the enclosing write's executor is initialized.
    run_cancelled_query scalar "INSERT INTO $DB.dst SELECT (
        SELECT sum(number) FROM numbers(1000000) WHERE sleepEachRow(0.0001)=0)" "$analyzer"
    # An incomplete `Set` is rejected semantically with `QUERY_WAS_CANCELLED` (394),
    # instead of the native `Cancel` transport exception `QUERY_WAS_CANCELLED_BY_CLIENT` (735).
    run_cancelled_query set "INSERT INTO $DB.dst SELECT n FROM $DB.source WHERE n IN ($SLOW)" "$analyzer" 394
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM $DB.dst"
done
