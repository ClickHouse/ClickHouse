#!/usr/bin/env bash
# Tags: no-ordinary-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./04666_run_query_in_background.lib
. "$CUR_DIR"/04666_run_query_in_background.lib

function run_http()
{
    local query=$1 query_id=${2:-} run_in_background=${3:-1} user=${4:-}
    $CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&async_insert=0&run_query_in_background=${run_in_background}${query_id:+&query_id=$query_id}${user:+&user=$user}" -d "$query"
}

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n"

shared_native_and_http_tests run_http

echo "=== http ==="
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t"

echo '--- server-minted query_id is returned in the header and inline body data is inserted'
headers=$($CLICKHOUSE_CURL -sS -D - -o /dev/null "${CLICKHOUSE_URL}&run_query_in_background=1&async_insert=0" -d "INSERT INTO t VALUES (1000), (1001)")
http_id=$(echo "$headers" | grep -i '^X-ClickHouse-Query-Id:' | tr -d '\r' | awk '{print $2}')
[[ -n "$http_id" ]] && echo "query_id header present"
wait_for_query_log "$(finished_in_query_log "$http_id")"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t WHERE n >= 1000"

echo '--- inline SETTINGS is rejected synchronously'
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}" -d "SELECT 1 SETTINGS run_query_in_background = 1" | grep -o -m1 "BAD_ARGUMENTS"

echo '--- a body that streams data beyond the query text is rejected synchronously'
{ echo "INSERT INTO t FORMAT TSV"; seq 1 300000; } | $CLICKHOUSE_CURL -sS -H "Expect:" "${CLICKHOUSE_URL}&run_query_in_background=1&async_insert=0" --data-binary @- | grep -o -m1 "BAD_ARGUMENTS"

background="background_${CLICKHOUSE_DATABASE}"
foreground="foreground_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "
    CREATE SETTINGS PROFILE ${background} SETTINGS run_query_in_background = 1;
    CREATE SETTINGS PROFILE ${foreground} SETTINGS run_query_in_background = 0;
"

echo '--- a profile in the URL enables the setting'
background_id="background_${CLICKHOUSE_DATABASE}"
out=$($CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&profile=${background}&query_id=${background_id}" -d "SELECT 1")
[[ -z "$out" ]] && echo "no output"
wait_for_query_log "$(finished_in_query_log "$background_id")"

echo '--- a profile in the URL disables the setting'
foreground_id="foreground_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&run_query_in_background=1&profile=${foreground}&query_id=${foreground_id}" -d "SELECT 2"
wait_for_query_log "$(finished_in_query_log "$foreground_id")"

echo '--- a profile in the SETTINGS clause of the query is rejected synchronously'
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&run_query_in_background=1" -d "SELECT 3 SETTINGS profile = '${foreground}'" \
    | grep -o -m1 "run_query_in_background cannot be changed in the SETTINGS clause of the query over HTTP"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}" -d "SELECT 3 SETTINGS profile = '${background}'" \
    | grep -o -m1 "run_query_in_background cannot be changed in the SETTINGS clause of the query over HTTP"

$CLICKHOUSE_CLIENT -q "
    DROP SETTINGS PROFILE ${background};
    DROP SETTINGS PROFILE ${foreground};
    DROP TABLE t;
"
