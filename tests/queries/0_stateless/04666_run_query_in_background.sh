#!/usr/bin/env bash
# Tags: no-ordinary-database, zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function wait_for()
{
    local condition=$1
    local start=$EPOCHSECONDS
    while [[ $($CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log; SELECT $condition" 2>/dev/null) != 1 ]]; do
        if ((EPOCHSECONDS - start > 180)); then
            echo "Timeout waiting for: $condition" >&2
            exit 1
        fi
        sleep 0.3
    done
}

function finished_in_query_log()
{
    echo "count() = 1 FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$1' AND type = 'QueryFinish'"
}

function run_native()
{
    local query=$1 query_id=${2:-} run_in_background=${3:-1}
    $CLICKHOUSE_CLIENT --async_insert 0 --run_query_in_background "$run_in_background" \
        ${query_id:+--query_id "$query_id"} -q "$query" 2>&1
}

function run_http()
{
    local query=$1 query_id=${2:-} run_in_background=${3:-1}
    $CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&async_insert=0&run_query_in_background=${run_in_background}${query_id:+&query_id=$query_id}" -d "$query"
}

function shared_native_and_http_tests()
{
    local run=$1
    echo "=== $run ==="
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t"

    echo '--- INSERT SELECT in background: no output, rows land, attributed in query_log'
    local insert_id="insert_${run}_${CLICKHOUSE_DATABASE}"
    local out
    out=$($run "INSERT INTO t SETTINGS max_block_size = 1 SELECT number FROM numbers(30) WHERE NOT ignore(sleepEachRow(0.05))" "$insert_id")
    [[ -z "$out" ]] && echo "no output"
    wait_for "$(finished_in_query_log "$insert_id")"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM t"
    $CLICKHOUSE_CLIENT -q "SELECT memory_usage > 0, length(ProfileEvents) > 0 FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$insert_id' AND type = 'QueryFinish'"
    wait_for "count() = 1 FROM system.background_queries WHERE query_id = '$insert_id' AND status = 'Finished'"
    $CLICKHOUSE_CLIENT -q "SELECT user = currentUser(), host != '', query != '', exception_code, exception = '', finish_time >= submit_time FROM system.background_queries WHERE query_id = '$insert_id'"

    echo '--- a long-running background query is visible and attributed in system.processes, and KILL QUERY kills it'
    local victim_id="victim_${run}_${CLICKHOUSE_DATABASE}"
    $run "INSERT INTO t SETTINGS max_block_size = 1 SELECT number FROM numbers(600) WHERE NOT ignore(sleepEachRow(0.1))" "$victim_id"
    wait_for "count() = 1 FROM system.processes WHERE query_id = '$victim_id' AND user = currentUser() AND current_database = currentDatabase() AND read_rows > 0"
    wait_for "count() = 1 FROM system.background_queries WHERE query_id = '$victim_id' AND status = 'Running' AND finish_time IS NULL"
    echo "attributed in system.processes and system.background_queries"
    out=$($run "KILL QUERY WHERE query_id = '$victim_id' SYNC")
    [[ -z "$out" ]] && echo "no output"
    wait_for "count() = 1 FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$victim_id' AND type = 'ExceptionWhileProcessing' AND exception_code = 394"
    wait_for "count() = 1 FROM system.background_queries WHERE query_id = '$victim_id' AND status = 'Failed' AND exception_code = 394 AND exception != ''"
    echo "killed"

    echo '--- SELECT in background discards the result'
    out=$($run "SELECT 1")
    [[ -z "$out" ]] && echo "no output"

    echo '--- CREATE TABLE AS SELECT in background'
    $run "CREATE TABLE t_ctas_${run} ENGINE = MergeTree ORDER BY n AS SELECT number AS n FROM numbers(10)"
    wait_for "count() = 10 FROM t_ctas_${run}"
    echo "created with rows"
    $CLICKHOUSE_CLIENT -q "DROP TABLE t_ctas_${run}"

    echo '--- enabling the setting for a whole session is rejected'
    $run "SET run_query_in_background = 1" "" 0 | grep -o -m1 "run_query_in_background cannot be changed with SET"

    echo '--- a query that cannot run in the background fails asynchronously'
    local set_id="set_${run}_${CLICKHOUSE_DATABASE}"
    $run "SET max_threads = 4" "$set_id"
    wait_for "count() = 1 FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$set_id' AND exception_code != 0"
    echo "failed asynchronously"
}

function native_tests()
{
    echo "=== native ==="
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t"

    echo '--- an INSERT whose data streams over the connection is rejected synchronously'
    echo "1" | run_native "INSERT INTO t FORMAT TSV" | grep -o -m1 "BAD_ARGUMENTS"

    echo '--- an INSERT reading its data from input() is rejected synchronously'
    echo "1" | run_native "INSERT INTO t SELECT * FROM input('n UInt64') FORMAT TSV" | grep -o -m1 "BAD_ARGUMENTS"

    echo '--- transactions are rejected synchronously'
    $CLICKHOUSE_CLIENT -q "
        BEGIN TRANSACTION;
        INSERT INTO t SETTINGS run_query_in_background = 1 SELECT 1;
    " 2>&1 | grep -o -m1 "Background queries inside transactions are not supported"
    $CLICKHOUSE_CLIENT -q "INSERT INTO t SETTINGS run_query_in_background = 1, implicit_transaction = 1 SELECT 1" 2>&1 \
        | grep -o -m1 "Background queries with 'implicit_transaction' are not supported"

    echo '--- a secondary query is rejected synchronously'
    $CLICKHOUSE_CLIENT --query_kind secondary_query --run_query_in_background 1 -q "SELECT 1" 2>&1 \
        | grep -o -m1 "run_query_in_background cannot be used for a secondary query"

    echo '--- a query processing stage other than Complete is rejected synchronously'
    $CLICKHOUSE_CLIENT --stage with_mergeable_state --run_query_in_background 1 -q "SELECT 1" 2>&1 \
        | grep -o -m1 "run_query_in_background cannot be used with the WithMergeableState query processing stage"

    echo '--- distributed INSERT in background: shards run in the foreground, all rows land'
    $CLICKHOUSE_CLIENT -q "CREATE TABLE t_dist (n UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t, rand())"
    local dist_id="dist_${CLICKHOUSE_DATABASE}"
    $CLICKHOUSE_CLIENT --query_id "$dist_id" -q "INSERT INTO t_dist SETTINGS run_query_in_background = 1, distributed_foreground_insert = 1 SELECT number + 2000 FROM numbers(100)"
    wait_for "$(finished_in_query_log "$dist_id")"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM t WHERE n >= 2000"
    $CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.query_log
        WHERE event_date >= yesterday() AND initial_query_id = '$dist_id' AND is_initial_query = 0 AND type = 'QueryStart'
            AND query_id NOT IN (
                SELECT query_id FROM system.query_log
                WHERE event_date >= yesterday() AND initial_query_id = '$dist_id' AND is_initial_query = 0 AND type = 'QueryFinish')"
    $CLICKHOUSE_CLIENT -q "DROP TABLE t_dist"
}

function http_tests()
{
    echo "=== http ==="
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t"

    echo '--- server-minted query_id is returned in the header and inline body data is inserted'
    local headers http_id
    headers=$($CLICKHOUSE_CURL -sS -D - -o /dev/null "${CLICKHOUSE_URL}&run_query_in_background=1&async_insert=0" -d "INSERT INTO t VALUES (1000), (1001)")
    http_id=$(echo "$headers" | grep -i '^X-ClickHouse-Query-Id:' | tr -d '\r' | awk '{print $2}')
    [[ -n "$http_id" ]] && echo "query_id header present"
    wait_for "$(finished_in_query_log "$http_id")"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM t WHERE n >= 1000"

    echo '--- inline SETTINGS is rejected synchronously'
    $CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}" -d "SELECT 1 SETTINGS run_query_in_background = 1" | grep -o -m1 "BAD_ARGUMENTS"

    echo '--- a body that streams data beyond the query text is rejected synchronously'
    { echo "INSERT INTO t FORMAT TSV"; seq 1 300000; } | $CLICKHOUSE_CURL -sS -H "Expect:" "${CLICKHOUSE_URL}&run_query_in_background=1&async_insert=0" --data-binary @- | grep -o -m1 "BAD_ARGUMENTS"
}

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n"

shared_native_and_http_tests run_native
shared_native_and_http_tests run_http
native_tests
http_tests

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
