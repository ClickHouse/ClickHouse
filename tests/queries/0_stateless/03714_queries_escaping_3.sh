#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: dealing with an SQLite database makes concurrent SHOW TABLES queries fail sporadically with the "database is locked" error.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

export CURR_DATABASE="test_03714_sqllite_${CLICKHOUSE_DATABASE}"

DB_PATH=${USER_FILES_PATH}/${CURR_DATABASE}_db1

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS ${CURR_DATABASE}"
}
trap cleanup EXIT


sqlite3 "${DB_PATH}" 'DROP TABLE IF EXISTS table1'

sqlite3 "${DB_PATH}" 'CREATE TABLE table1 (col1 text, col2 smallint);'

chmod ugo+w "${DB_PATH}"

sqlite3 "${DB_PATH}" "INSERT INTO table1 VALUES ('line1', 1), ('line2', 2), ('line3', 3)"

${CLICKHOUSE_CLIENT} --query="CREATE DATABASE ${CURR_DATABASE} ENGINE = SQLite('${DB_PATH}')"

${CLICKHOUSE_CLIENT} --query="EXISTS TABLE ${CURR_DATABASE}.table1;"
${CLICKHOUSE_CLIENT} --query="EXISTS TABLE ${CURR_DATABASE}.\"a\' or name='table1\";"


${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS ${CURR_DATABASE}"
