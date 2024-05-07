#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# See 01658_read_file_to_string_column.sh
user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS ${CURR_DATABASE}"
    rm -r "${DB_PATH}"
}
trap cleanup EXIT

export CURR_DATABASE="test_01889_sqllite_${CLICKHOUSE_DATABASE}"

DB_PATH=${user_files_path}/${CURR_DATABASE}_db1

${CLICKHOUSE_CLIENT} --multiquery --multiline --query="""
DROP DATABASE IF EXISTS ${CURR_DATABASE};
CREATE DATABASE ${CURR_DATABASE} ENGINE = SQLite('${DB_PATH}');
SHOW TABLES FROM ${CURR_DATABASE};
"""

sqlite3 "${DB_PATH}" 'CREATE TABLE table1 (col1 text, col2 smallint);'

${CLICKHOUSE_CLIENT} --multiquery --multiline --query="""
SHOW TABLES FROM ${CURR_DATABASE};
"""
