#!/usr/bin/env bash
# Tags: no-ordinary-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function wait_for()
{
    local condition=$1
    local start=$EPOCHSECONDS
    while [[ $($CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log; SELECT $condition" 2>/dev/null) != 1 ]]; do
        if ((EPOCHSECONDS - start > 120)); then
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

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n"

echo '--- INSERT SELECT in background: no output, visible and attributed in system.processes, rows land'
insert_id="insert_${CLICKHOUSE_DATABASE}"
out=$($CLICKHOUSE_CLIENT --query_id "$insert_id" -q "INSERT INTO t SETTINGS run_query_in_background = 1, max_block_size = 1 SELECT number FROM numbers(30) WHERE NOT ignore(sleepEachRow(0.05))")
[[ -z "$out" ]] && echo "no output"
wait_for "count() = 1 FROM system.processes WHERE query_id = '$insert_id' AND peak_memory_usage > 0 AND read_rows > 0"
echo "attributed in system.processes"
wait_for "$(finished_in_query_log $insert_id)"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t"
$CLICKHOUSE_CLIENT -q "SELECT memory_usage > 0, length(ProfileEvents) > 0 FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$insert_id' AND type = 'QueryFinish'"

echo '--- KILL QUERY in background kills a background query'
victim_id="victim_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$victim_id" -q "INSERT INTO t SETTINGS run_query_in_background = 1, max_block_size = 1 SELECT number FROM numbers(600) WHERE NOT ignore(sleepEachRow(0.1))"
wait_for "count() = 1 FROM system.processes WHERE query_id = '$victim_id'"
out=$($CLICKHOUSE_CLIENT --run_query_in_background 1 -q "KILL QUERY WHERE query_id = '$victim_id' SYNC")
[[ -z "$out" ]] && echo "no output"
wait_for "count() = 1 FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$victim_id' AND type = 'ExceptionWhileProcessing' AND exception_code = 394"
echo "killed"

echo '--- SELECT in background discards the result'
$CLICKHOUSE_CLIENT --run_query_in_background 1 -q "SELECT 1"

echo '--- CREATE TABLE AS SELECT in background'
$CLICKHOUSE_CLIENT --run_query_in_background 1 -q "CREATE TABLE t_ctas ENGINE = MergeTree ORDER BY n AS SELECT number AS n FROM numbers(10)"
wait_for "count() = 10 FROM t_ctas"
echo "created with rows"

echo '--- HTTP URL parameter: dispatched, server-minted query_id returned in the header, inline body data inserted'
headers=$($CLICKHOUSE_CURL -sS -D - -o /dev/null "${CLICKHOUSE_URL}&run_query_in_background=1&async_insert=0" -d "INSERT INTO t VALUES (1000), (1001)")
http_id=$(echo "$headers" | grep -i '^X-ClickHouse-Query-Id:' | tr -d '\r' | awk '{print $2}')
[[ -n "$http_id" ]] && echo "query_id header present"
wait_for "$(finished_in_query_log $http_id)"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t WHERE n >= 1000"

echo '--- inline SETTINGS over HTTP is rejected synchronously'
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}" -d "SELECT 1 SETTINGS run_query_in_background = 1" | grep -o -m1 "BAD_ARGUMENTS"

echo '--- HTTP body that streams data beyond the query text is rejected synchronously'
{ echo "INSERT INTO t FORMAT TSV"; seq 1 300000; } | $CLICKHOUSE_CURL -sS -H "Expect:" "${CLICKHOUSE_URL}&run_query_in_background=1&async_insert=0" --data-binary @- | grep -o -m1 "BAD_ARGUMENTS"

echo '--- queries that cannot work in the background fail asynchronously, inserting nothing'
native_data_id="native_data_${CLICKHOUSE_DATABASE}"
echo "1" | $CLICKHOUSE_CLIENT --query_id "$native_data_id" --run_query_in_background 1 --async_insert 0 -q "INSERT INTO t FORMAT TSV" > /dev/null 2>&1 || true
wait_for "count() = 1 FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$native_data_id' AND exception_code != 0"
set_id="set_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&run_query_in_background=1&query_id=${set_id}" -d "SET max_threads = 4"
wait_for "count() = 1 FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$set_id' AND exception_code != 0"
echo "failed asynchronously"

echo '--- transactions are rejected synchronously'
$CLICKHOUSE_CLIENT -q "
    BEGIN TRANSACTION;
    INSERT INTO t SETTINGS run_query_in_background = 1 SELECT 1;
" 2>&1 | grep -o -m1 "Background queries inside transactions are not supported"
$CLICKHOUSE_CLIENT -q "INSERT INTO t SETTINGS run_query_in_background = 1, implicit_transaction = 1 SELECT 1" 2>&1 \
    | grep -o -m1 "Background queries with 'implicit_transaction' are not supported"

echo '--- distributed INSERT in background: shards run in the foreground, all rows land'
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_dist (n UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t, rand())"
dist_id="dist_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$dist_id" -q "INSERT INTO t_dist SETTINGS run_query_in_background = 1, distributed_foreground_insert = 1 SELECT number + 2000 FROM numbers(100)"
wait_for "$(finished_in_query_log $dist_id)"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t WHERE n >= 2000"
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM system.query_log
    WHERE event_date >= yesterday() AND initial_query_id = '$dist_id' AND is_initial_query = 0 AND type = 'QueryStart'
        AND query_id NOT IN (
            SELECT query_id FROM system.query_log
            WHERE event_date >= yesterday() AND initial_query_id = '$dist_id' AND is_initial_query = 0 AND type = 'QueryFinish')"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_dist"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_ctas"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"
