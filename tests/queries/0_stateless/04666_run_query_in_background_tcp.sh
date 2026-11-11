#!/usr/bin/env bash
# Tags: no-ordinary-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./04666_run_query_in_background.lib
. "$CUR_DIR"/04666_run_query_in_background.lib

function run_native()
{
    local query=$1 query_id=${2:-} run_in_background=${3:-1} user=${4:-}
    $CLICKHOUSE_CLIENT --async_insert 0 --run_query_in_background "$run_in_background" \
        ${user:+--user "$user"} ${query_id:+--query_id "$query_id"} -q "$query" 2>&1
}

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n"

shared_native_and_http_tests run_native

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

echo '--- the SETTINGS clause of a CREATE with a storage definition is rejected synchronously'
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_create_settings (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS run_query_in_background = 1" 2>&1 \
    | grep -o -m1 "run_query_in_background cannot be changed in the SETTINGS clause of this particular query"

echo '--- a secondary query is rejected synchronously'
$CLICKHOUSE_CLIENT --query_kind secondary_query --run_query_in_background 1 -q "SELECT 1" 2>&1 \
    | grep -o -m1 "run_query_in_background cannot be used for a secondary query"

echo '--- a query processing stage other than Complete is rejected synchronously'
$CLICKHOUSE_CLIENT --stage with_mergeable_state --run_query_in_background 1 -q "SELECT 1" 2>&1 \
    | grep -o -m1 "run_query_in_background cannot be used with the WithMergeableState query processing stage"

echo '--- a settings constraint the session applied is enforced synchronously'
profile="profile_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE $profile SETTINGS max_result_rows = 4 CONST"
$CLICKHOUSE_CLIENT -q "SET profile = '$profile'; SELECT 1 SETTINGS max_result_rows = 8, run_query_in_background = 1" 2>&1 \
    | grep -o -m1 'SETTING_CONSTRAINT_VIOLATION'
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $profile"

echo '--- distributed INSERT in background: shards run in the foreground, all rows land'
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_dist (n UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t, rand())"
dist_id="dist_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$dist_id" -q "INSERT INTO t_dist SETTINGS run_query_in_background = 1, distributed_foreground_insert = 1 SELECT number + 2000 FROM numbers(100)"
wait_for_query_log "$(finished_in_query_log "$dist_id")"
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM t WHERE n >= 2000;
    SELECT count() FROM system.query_log
    WHERE event_date >= yesterday() AND has(databases, currentDatabase()) AND initial_query_id = '$dist_id'
        AND is_initial_query = 0 AND type = 'QueryStart'
        AND query_id NOT IN (
            SELECT query_id FROM system.query_log
            WHERE event_date >= yesterday() AND initial_query_id = '$dist_id' AND is_initial_query = 0 AND type = 'QueryFinish');
    DROP TABLE t_dist"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
