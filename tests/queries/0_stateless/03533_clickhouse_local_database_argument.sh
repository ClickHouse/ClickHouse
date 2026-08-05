#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --path "${CLICKHOUSE_TMP}" --query "
CREATE DATABASE ${CLICKHOUSE_DATABASE_1};
USE ${CLICKHOUSE_DATABASE_1};
CREATE TABLE t (s String) ORDER BY ();
INSERT INTO t VALUES ('Hello, world');
SELECT * FROM t;
"

# We can switch to the previously created database using a command-line argument:

$CLICKHOUSE_LOCAL --path "${CLICKHOUSE_TMP}" --query "SELECT * FROM ${CLICKHOUSE_DATABASE_1}.t;"
$CLICKHOUSE_LOCAL --path "${CLICKHOUSE_TMP}" --query "USE ${CLICKHOUSE_DATABASE_1}; SELECT * FROM t;"
$CLICKHOUSE_LOCAL --path "${CLICKHOUSE_TMP}" --database default --query "USE ${CLICKHOUSE_DATABASE_1}; SELECT * FROM t;"
$CLICKHOUSE_LOCAL --path "${CLICKHOUSE_TMP}" --database ${CLICKHOUSE_DATABASE_1} --query "SELECT * FROM t;"
$CLICKHOUSE_LOCAL --path "${CLICKHOUSE_TMP}" --database system --query "USE ${CLICKHOUSE_DATABASE_1}; SELECT * FROM t;"

# Only default database is configured as a filesystem overlay:

echo "Hello from a file" > "${CLICKHOUSE_TMP}/file.csv"

$CLICKHOUSE_LOCAL --path "${CLICKHOUSE_TMP}" --query "SELECT * FROM '${CLICKHOUSE_TMP}/file.csv'"
$CLICKHOUSE_LOCAL --path "${CLICKHOUSE_TMP}" --query "SELECT * FROM default.\`${CLICKHOUSE_TMP}/file.csv\`"
$CLICKHOUSE_LOCAL --path "${CLICKHOUSE_TMP}" --database ${CLICKHOUSE_DATABASE_1} --query "SELECT * FROM default.\`${CLICKHOUSE_TMP}/file.csv\`"

$CLICKHOUSE_LOCAL --path "${CLICKHOUSE_TMP}" --query "DROP DATABASE ${CLICKHOUSE_DATABASE_1};"
