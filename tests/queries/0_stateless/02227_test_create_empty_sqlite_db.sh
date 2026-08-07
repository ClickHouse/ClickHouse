#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: dealing with an SQLite database makes concurrent SHOW TABLES queries fail sporadically with the "database is locked" error.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS ${CURR_DATABASE}"
    rm -r "${DB_PATH}"
}
trap cleanup EXIT

export CURR_DATABASE="test_01889_sqllite_${CLICKHOUSE_DATABASE}"

DB_PATH=${USER_FILES_PATH}/${CURR_DATABASE}_db1

${CLICKHOUSE_CLIENT} --multiline --query="""
DROP DATABASE IF EXISTS ${CURR_DATABASE};
CREATE DATABASE ${CURR_DATABASE} ENGINE = SQLite('${DB_PATH}');
SHOW TABLES FROM ${CURR_DATABASE};
"""

sqlite3 "${DB_PATH}" 'CREATE TABLE table1 (col1 text, col2 smallint);'

${CLICKHOUSE_CLIENT} --multiline --query="""
SHOW TABLES FROM ${CURR_DATABASE};
"""
