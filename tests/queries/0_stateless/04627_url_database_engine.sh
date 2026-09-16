#!/usr/bin/env bash

# Test the URL database engine and the URL support in the default (Overlay) database of clickhouse-local:
# `SELECT * FROM 'https://example.com/data.csv'`. The default database of clickhouse-local resolves
# table names against a `file://<cwd>/` base URL, so plain file names and URLs are handled uniformly.
# The queries read from the HTTP interface of the test server, e.g. the `/ping` endpoint that returns `Ok.`.
# https://github.com/ClickHouse/ClickHouse/issues/59617

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SERVER_URL="http://localhost:${CLICKHOUSE_PORT_HTTP}"

echo '--- a URL as a table name in clickhouse-local'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM '${SERVER_URL}/ping'"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM '${SERVER_URL}/?query=SELECT+number+FROM+numbers(3)+FORMAT+TSV' SETTINGS use_hive_partitioning = 0"

echo '--- a file:// URL as a table name in clickhouse-local'
DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"
printf '1,one\n2,two\n' > "${DATA_FILE}"
DATA_FILE_ABS=$(realpath "${DATA_FILE}")
${CLICKHOUSE_LOCAL} -q "SELECT * FROM 'file://${DATA_FILE_ABS}'"

echo '--- plain file names are resolved via the file:// base URL of the default database'
(cd "${CLICKHOUSE_TMP}" && ${CLICKHOUSE_LOCAL} -q "SELECT count() FROM '${CLICKHOUSE_TEST_UNIQUE_NAME}.csv'")

echo '--- glob patterns in plain file names'
printf '3,three\n' > "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_g_1.csv"
printf '4,four\n' > "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_g_2.csv"
(cd "${CLICKHOUSE_TMP}" && ${CLICKHOUSE_LOCAL} -q "SELECT count() FROM '${CLICKHOUSE_TEST_UNIQUE_NAME}_g_*.csv'")
(cd "${CLICKHOUSE_TMP}" && ${CLICKHOUSE_LOCAL} -q "SELECT count() FROM '${CLICKHOUSE_TEST_UNIQUE_NAME}_g_?.csv'")
rm "${DATA_FILE}" "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_g_1.csv" "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_g_2.csv"

echo '--- unknown table names still produce UNKNOWN_TABLE'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM no_such_table_04627" 2>&1 | grep -oF 'UNKNOWN_TABLE' | head -1
${CLICKHOUSE_LOCAL} -q "SELECT * FROM 'localhost:9000'" 2>&1 | grep -oF 'UNKNOWN_TABLE' | head -1

echo '--- CREATE TABLE in clickhouse-local is not shadowed by the URL database'
${CLICKHOUSE_LOCAL} -q "CREATE TABLE t04627 (n UInt8) ENGINE = Memory; INSERT INTO t04627 VALUES (1); SELECT * FROM t04627;"

echo '--- URL database engine with a base URL in clickhouse-local'
${CLICKHOUSE_LOCAL} -q "
CREATE DATABASE web ENGINE = URL('${SERVER_URL}/');
SELECT * FROM web.\`ping\`;
SHOW CREATE DATABASE web FORMAT TabSeparatedRaw;
" | sed "s|${SERVER_URL}|http://server|"

echo '--- URL database engine on the server'
WEB_DB="${CLICKHOUSE_DATABASE}_04627_web"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${WEB_DB}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${WEB_DB} ENGINE = URL('${SERVER_URL}/')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${WEB_DB}.\`ping\`"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${WEB_DB}.\`${SERVER_URL}/ping\`"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${WEB_DB}"

echo '--- the base URL must contain a scheme'
${CLICKHOUSE_LOCAL} -q "CREATE DATABASE bad ENGINE = URL('localhost/dir/')" 2>&1 | grep -oF 'must contain a scheme' | head -1
