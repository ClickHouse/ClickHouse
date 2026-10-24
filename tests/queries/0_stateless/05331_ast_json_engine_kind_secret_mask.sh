#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the S3 and MySQL engines are not in the fast test build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Secret masking picks its rule from the function kind. A `clickhouse_json` payload that omits or changes the kind of an
# engine, or puts an engine or `CODEC` kind on a table function, must be logged masked exactly like the SQL statement.

S3_URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}.csv"
PREFIX="${CLICKHOUSE_DATABASE}_05331_"

# run <case> <SQL> <from> <to>: execute the JSON of <SQL> with <from> replaced by <to>.
function run()
{
    local orig json
    orig=$($CLICKHOUSE_CLIENT -q "SELECT parseQueryToJSON(\$\$$2\$\$) FORMAT TSVRaw")
    json=${orig//"$3"/"$4"}
    [ "$json" != "$orig" ] || echo "$1: the payload was not edited"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json&enable_json_ast_dialect=1&log_queries=1&query_id=${PREFIX}$1" \
        --data-binary "$json" > /dev/null 2>&1
}

TBL_S3="CREATE TABLE t_s3 (x Int32) ENGINE = S3('$S3_URL', 'AKID', 'SEKRIT_1', 'CSV')"
run tbl_s3_nokind "$TBL_S3" ',"kind":"TABLE_ENGINE"' ''
run tbl_mysql_dbkind "CREATE TABLE t_my (x Int32) ENGINE = MySQL('127.0.0.1:1', 'd', 't', 'u', 'SEKRIT_2')" '"kind":"TABLE_ENGINE"' '"kind":"DATABASE_ENGINE"'
run tbl_s3_windowkind "CREATE TABLE t_s3w (x Int32) ENGINE = S3('$S3_URL', 'AKID', 'SEKRIT_7', 'CSV')" '"kind":"TABLE_ENGINE"' '"kind":"WINDOW_FUNCTION"'
run mv_s3_nokind "CREATE MATERIALIZED VIEW mv (x Int32) ENGINE = S3('$S3_URL', 'AKID', 'SEKRIT_3', 'CSV') AS SELECT 1 AS x" ',"kind":"TABLE_ENGINE"' ''
run db_mysql_nokind "CREATE DATABASE ${CLICKHOUSE_DATABASE}_my ENGINE = MySQL('127.0.0.1:1', 'd', 'u', 'SEKRIT_4')" ',"kind":"DATABASE_ENGINE"' ''
run tf_engine_kind "SELECT * FROM mysql('127.0.0.1:1', 'd', 't', 'u', 'SEKRIT_5')" '"name":"mysql"' '"name":"mysql","kind":"TABLE_ENGINE"'
run tf_codec_kind "SELECT * FROM mysql('127.0.0.1:1', 'd', 't', 'u', 'SEKRIT_6')" '"name":"mysql"' '"name":"mysql","kind":"CODEC"'

# The kind-less payload is the same statement, secret included.
JSON=$($CLICKHOUSE_CLIENT -q "SELECT replaceAll(parseQueryToJSON(\$\$$TBL_S3\$\$), ',\"kind\":\"TABLE_ENGINE\"', '') FORMAT TSVRaw")
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON(\$\$$JSON\$\$) = formatQuerySingleLine(\$\$$TBL_S3\$\$)"

for _ in {1..60}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    FINISHED=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log
        WHERE current_database = currentDatabase() AND startsWith(query_id, '$PREFIX') AND type != 'QueryStart'")
    [ "$FINISHED" -ge 7 ] && break
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "
    SELECT replaceOne(query_id, '$PREFIX', '') AS c, count() > 0, countIf(position(query, 'SEKRIT') > 0),
        countIf(position(query, '[HIDDEN]') > 0) = count()
    FROM system.query_log
    WHERE current_database = currentDatabase() AND startsWith(query_id, '$PREFIX')
    GROUP BY c ORDER BY c"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_s3; DROP TABLE IF EXISTS t_s3w; DROP TABLE IF EXISTS t_my; DROP TABLE IF EXISTS mv; DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_my"
