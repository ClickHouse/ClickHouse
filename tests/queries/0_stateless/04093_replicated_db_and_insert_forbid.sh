#!/usr/bin/env bash
# Tags: replica, zookeeper

# Test for CREATE TABLE IF NOT EXISTS ... AND INSERT restrictions with ReplicatedMergeTree
# in a Replicated database.

set -e -o pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query \
    "CREATE DATABASE ${CLICKHOUSE_DATABASE}_rmt_db ENGINE = Replicated('/clickhouse/databases/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_DATABASE}_rmt_db', '{shard}', '{replica}')" || exit 1

trap '${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_rmt_db"' EXIT

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query \
    "CREATE TABLE ${CLICKHOUSE_DATABASE}_rmt_db.rmt_table (a UInt32, b String) ENGINE = ReplicatedMergeTree ORDER BY a" || exit 1

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query \
    "CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}_rmt_db.rmt_table (a UInt32, b String) ENGINE = MergeTree ORDER BY a AND INSERT SELECT 1 AS a, 'test' AS b; -- { serverError SUPPORT_IS_DISABLED }" || exit 1

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_heavy_create=1 --query \
    "CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}_rmt_db.rmt_table (a UInt32, b String) ENGINE = MergeTree ORDER BY a AND INSERT SELECT 2 AS a, 'test2' AS b; -- { serverError SUPPORT_IS_DISABLED }" || exit 1

${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM ${CLICKHOUSE_DATABASE}_rmt_db.rmt_table" || exit 1

cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP TABLE IF EXISTS t_04093_stream;
        DROP TABLE IF EXISTS t_04093_http;
        DROP TABLE IF EXISTS t_04093_http_input;
        DROP TABLE IF EXISTS t_04093_delimiter;
        DROP TABLE IF EXISTS t_04093_storage_delimiter;
        DROP TABLE IF EXISTS t_04093_ambiguous;
        DROP TABLE IF EXISTS t_04093_empty_as;
        DROP TABLE IF EXISTS t_04093_empty_and;
        DROP TABLE IF EXISTS t_04093_empty_as_select;
        DROP TABLE IF EXISTS t_04093_empty_and_select;
        DROP TABLE IF EXISTS t_04093_large;
        DROP TABLE IF EXISTS t_04093_concurrent;
        DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_rmt_db;
    "
}
trap cleanup EXIT
${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP TABLE IF EXISTS t_04093_stream;
    DROP TABLE IF EXISTS t_04093_http;
    DROP TABLE IF EXISTS t_04093_http_input;
    DROP TABLE IF EXISTS t_04093_delimiter;
    DROP TABLE IF EXISTS t_04093_storage_delimiter;
    DROP TABLE IF EXISTS t_04093_ambiguous;
    DROP TABLE IF EXISTS t_04093_empty_as;
    DROP TABLE IF EXISTS t_04093_empty_and;
    DROP TABLE IF EXISTS t_04093_empty_as_select;
    DROP TABLE IF EXISTS t_04093_empty_and_select;
    DROP TABLE IF EXISTS t_04093_large;
    DROP TABLE IF EXISTS t_04093_concurrent;
"

printf '1\n2\n99\n' | ${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE t_04093_stream (x UInt8) ENGINE = Memory AS INSERT SELECT * FROM input('x UInt8') SETTINGS limit = 2 FORMAT CSV"
printf '3\n4\n99\n' | ${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE IF NOT EXISTS t_04093_stream (x UInt8) ENGINE = Memory AND INSERT SELECT * FROM input('x UInt8') SETTINGS limit = 2 FORMAT CSV"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM t_04093_stream"

${CLICKHOUSE_CLIENT} --throw_if_no_data_to_insert=0 --query \
    "CREATE TABLE t_04093_empty_as (x UInt8) ENGINE = Memory AS INSERT FORMAT CSV" < /dev/null
${CLICKHOUSE_CLIENT} --throw_if_no_data_to_insert=0 --query \
    "CREATE TABLE IF NOT EXISTS t_04093_empty_and (x UInt8) ENGINE = Memory AND INSERT FORMAT CSV" < /dev/null
${CLICKHOUSE_CLIENT} --throw_if_no_data_to_insert=0 --query \
    "CREATE TABLE t_04093_empty_as_select (x UInt8) ENGINE = Memory AS INSERT SELECT * FROM input('x UInt8') FORMAT CSV" < /dev/null
${CLICKHOUSE_CLIENT} --throw_if_no_data_to_insert=0 --query \
    "CREATE TABLE IF NOT EXISTS t_04093_empty_and_select (x UInt8) ENGINE = Memory AND INSERT SELECT * FROM input('x UInt8') FORMAT CSV" < /dev/null
for table in t_04093_empty_as t_04093_empty_and t_04093_empty_as_select t_04093_empty_and_select; do
    ${CLICKHOUSE_CLIENT} --query "EXISTS TABLE $table"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM $table"
done

printf '99\n' | ${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE IF NOT EXISTS t_04093_stream AS INSERT SELECT * FROM input('auto') FORMAT CSV"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04093_stream VALUES (5)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM t_04093_stream"

printf '%s\n' \
    "CREATE TABLE IF NOT EXISTS t_04093_stream AS INSERT SELECT * FROM input('auto') FORMAT CSV" \
    '99' | ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}" --data-binary @-
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04093_stream VALUES (6)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM t_04093_stream"

printf '%s\n' \
    'CREATE TABLE t_04093_http (x String) ENGINE = Memory AS INSERT FORMAT CSV' \
    'SETTINGS made_up_setting = 1' | \
    ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}" --data-binary @-
${CLICKHOUSE_CLIENT} --query "SELECT x FROM t_04093_http"

printf '%s\n' \
    "CREATE TABLE t_04093_http_input (x UInt8) ENGINE = Memory AS INSERT SELECT * FROM input('x UInt8') SETTINGS limit = 2 FORMAT CSV" \
    '1' '2' '99' | \
    ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}" --data-binary @-
printf '%s\n' \
    "CREATE TABLE IF NOT EXISTS t_04093_http_input (x UInt8) ENGINE = Memory AND INSERT SELECT * FROM input('x UInt8') SETTINGS limit = 2 FORMAT CSV" \
    '3' '4' '99' | \
    ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}" --data-binary @-
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM t_04093_http_input"

query_id_values="${CLICKHOUSE_DATABASE}_04093_explain_values_$$"
query_id_csv="${CLICKHOUSE_DATABASE}_04093_explain_csv_$$"
printf '%s' \
    "EXPLAIN SYNTAX CREATE TABLE t_04093_explain (x String) ENGINE = Memory AS INSERT VALUES ('payload_04093_secret_values')" | \
    ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}&log_queries=1&log_queries_probability=1&query_id=${query_id_values}" --data-binary @- > /dev/null
printf '%s\n' \
    'EXPLAIN SYNTAX CREATE TABLE t_04093_explain (x String) ENGINE = Memory AS INSERT FORMAT CSV' \
    'payload_04093_secret_csv' | \
    ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}&log_queries=1&log_queries_probability=1&query_id=${query_id_csv}" --data-binary @- > /dev/null
for attempt in {1..120}; do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    logged_queries=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count()
        FROM system.query_log
        WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
            AND query_id IN ('${query_id_values}', '${query_id_csv}')
    ")
    if [ "$logged_queries" -eq 2 ]; then
        break
    fi
    if [ "$attempt" -eq 120 ]; then
        echo "Timed out waiting for both EXPLAIN queries in query_log" >&2
        exit 1
    fi
    sleep 0.5
done
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), countIf(position(query, 'AS INSERT') > 0), countIf(position(query, 'payload_04093_secret') > 0)
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND query_id IN ('${query_id_values}', '${query_id_csv}')
"

${CLICKHOUSE_CLIENT} --multiquery <<'SQL' | grep -Ex '42|43'
EXPLAIN SYNTAX CREATE TABLE t_04093_explain (x UInt8) ENGINE = Memory AS INSERT VALUES (1), (2);
SELECT 42;
EXPLAIN SYNTAX CREATE TABLE IF NOT EXISTS t_04093_explain (x UInt8) ENGINE = Memory AND INSERT VALUES (1);
SELECT 43;
SQL

${CLICKHOUSE_CLIENT} --multiquery <<'SQL' | grep -Ex '44|45'
EXPLAIN SYNTAX CREATE TABLE t_04093_explain (x String) ENGINE = Memory AS INSERT FORMAT CSV
hello;world

SELECT 44;
EXPLAIN SYNTAX CREATE TABLE IF NOT EXISTS t_04093_explain (x String) ENGINE = Memory AND INSERT FORMAT CSV
hello;world

SELECT 45;
SQL

printf '1;2\n' | ${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE t_04093_delimiter (x UInt8, y UInt8) ENGINE = Memory AS INSERT SELECT * FROM input('x UInt8, y UInt8') SETTINGS format_csv_delimiter = ';' FORMAT CSV"
printf '3;4\n' | ${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE IF NOT EXISTS t_04093_delimiter AND INSERT SELECT * FROM input('x UInt8, y UInt8') SETTINGS format_csv_delimiter = ';' FORMAT CSV"
printf '%s\n' \
    "CREATE TABLE IF NOT EXISTS t_04093_delimiter AND INSERT SELECT * FROM input('x UInt8, y UInt8') SETTINGS format_csv_delimiter = ';' FORMAT CSV" \
    '5;6' | \
    ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}" --data-binary @-
${CLICKHOUSE_CLIENT} --query "SELECT x, y FROM t_04093_delimiter ORDER BY x"

printf 'a;b,c\n' | ${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE t_04093_storage_delimiter (x String, y String) ENGINE = Memory SETTINGS compress = 1, format_csv_delimiter = ';' AS INSERT FORMAT CSV"
printf 'd;e,f\n' | ${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE IF NOT EXISTS t_04093_storage_delimiter (x String, y String) ENGINE = Memory SETTINGS format_csv_delimiter = ';' AND INSERT FORMAT CSV"
printf 'g|h,i\n' | ${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE IF NOT EXISTS t_04093_storage_delimiter (x String, y String) ENGINE = Memory SETTINGS format_csv_delimiter = ';' AND INSERT SELECT * FROM input('x String, y String') SETTINGS format_csv_delimiter = '|' FORMAT CSV"
printf 'j,k;l\n' | ${CLICKHOUSE_CLIENT} --format_csv_delimiter=';' --query \
    "CREATE TABLE IF NOT EXISTS t_04093_storage_delimiter (x String, y String) ENGINE = Memory SETTINGS format_csv_delimiter = DEFAULT AND INSERT FORMAT CSV"
printf 'l;m,n\n' | ${CLICKHOUSE_CLIENT} --param_delimiter=';' --query \
    "CREATE TABLE IF NOT EXISTS t_04093_storage_delimiter (x String, y String) ENGINE = Memory SETTINGS format_csv_delimiter = {delimiter:String} AND INSERT FORMAT CSV"
printf 'r;s,t\n' | ${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE IF NOT EXISTS t_04093_storage_delimiter (x String, y String) ENGINE = Memory SETTINGS input_format = 'CSV', format_csv_delimiter = ';' AND INSERT SELECT * FROM input('x String, y String')"
printf '%s\n' \
    "CREATE TABLE IF NOT EXISTS t_04093_storage_delimiter (x String, y String) ENGINE = Memory SETTINGS format_csv_delimiter = ';' AND INSERT FORMAT CSV" \
    'o;p,q' | \
    ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}" --data-binary @-
${CLICKHOUSE_CLIENT} --query "SELECT x, y FROM t_04093_storage_delimiter ORDER BY x"

if error=$(printf 'a;b\n' | ${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE t_04093_ambiguous (x String, y String) SETTINGS format_csv_delimiter = ';' AS INSERT FORMAT CSV" 2>&1); then
    echo "Expected an error for storage settings without an explicit engine" >&2
    exit 1
fi
if [[ "$error" != *"ENGINE must be specified for CREATE with INSERT data and storage SETTINGS"* ]]; then
    echo "$error" >&2
    exit 1
fi
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE t_04093_ambiguous"

{
    printf 'CREATE TABLE t_04093_large (x UInt8) ENGINE = Memory AS INSERT FORMAT RowBinary\n'
    head -c 10000 /dev/zero
} | ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}&max_query_size=1024" --data-binary @-
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_04093_large"

${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE IF NOT EXISTS t_04093_concurrent (x UInt8) ENGINE = MergeTree ORDER BY x AND INSERT SELECT 1" &
first_pid=$!
${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE IF NOT EXISTS t_04093_concurrent (x UInt8) ENGINE = MergeTree ORDER BY x AND INSERT SELECT 2" &
second_pid=$!
status=0
wait "$first_pid" || status=1
wait "$second_pid" || status=1
if [ "$status" -ne 0 ]; then
    exit "$status"
fi
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM t_04093_concurrent"

(
    local_dir=$(mktemp -d "${CLICKHOUSE_TMP}/04093_local_XXXXXX")
    trap 'rm -rf -- "$local_dir"' EXIT

    printf '1,hello\n' | $CLICKHOUSE_LOCAL --path "$local_dir" --structure 'x UInt8, y String' \
        --query "CREATE TABLE as_insert (x UInt8, y String) ENGINE = MergeTree ORDER BY x AS INSERT SELECT * FROM input('x UInt8, y String') FORMAT CSV"
    $CLICKHOUSE_LOCAL --path "$local_dir" --query "SELECT x, y FROM as_insert"

    $CLICKHOUSE_LOCAL --path "$local_dir" --query "CREATE TABLE and_insert (x UInt8, y String) ENGINE = MergeTree ORDER BY x"
    printf '2,world\n' | $CLICKHOUSE_LOCAL --path "$local_dir" --structure 'x UInt8, y String' \
        --query "CREATE TABLE IF NOT EXISTS and_insert AND INSERT SELECT * FROM input('x UInt8, y String') FORMAT CSV"
    $CLICKHOUSE_LOCAL --path "$local_dir" --query "SELECT x, y FROM and_insert"

    printf '3\toverride\n' | $CLICKHOUSE_LOCAL --path "$local_dir" --structure 'x UInt8, y String' --input-format TSV \
        --query "CREATE TABLE IF NOT EXISTS and_insert AND INSERT SELECT * FROM input('x UInt8, y String') FORMAT CSV"
    $CLICKHOUSE_LOCAL --path "$local_dir" --query "SELECT x, y FROM and_insert ORDER BY x"
)