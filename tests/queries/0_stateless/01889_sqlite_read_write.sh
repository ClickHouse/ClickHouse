#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: dealing with an SQLite database makes concurrent SHOW TABLES queries fail sporadically with the "database is locked" error.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

export CURR_DATABASE="test_01889_sqllite_${CLICKHOUSE_DATABASE}"

DB_PATH=${USER_FILES_PATH}/${CURR_DATABASE}_db1
DB_PATH2=$CUR_DIR/${CURR_DATABASE}_db2

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS ${CURR_DATABASE}"
}
trap cleanup EXIT

sqlite3 "${DB_PATH}" 'DROP TABLE IF EXISTS table1'
sqlite3 "${DB_PATH}" 'DROP TABLE IF EXISTS table2'
sqlite3 "${DB_PATH}" 'DROP TABLE IF EXISTS table3'
sqlite3 "${DB_PATH}" 'DROP TABLE IF EXISTS table4'
sqlite3 "${DB_PATH}" 'DROP TABLE IF EXISTS table5'

sqlite3 "${DB_PATH}" 'CREATE TABLE table1 (col1 text, col2 smallint);'
sqlite3 "${DB_PATH}" 'CREATE TABLE table2 (col1 int, col2 text);'

chmod ugo+w "${DB_PATH}"

sqlite3 "${DB_PATH}" "INSERT INTO table1 VALUES ('line1', 1), ('line2', 2), ('line3', 3)"
sqlite3 "${DB_PATH}" "INSERT INTO table2 VALUES (1, 'text1'), (2, 'text2'), (3, 'text3')"

sqlite3 "${DB_PATH}" 'CREATE TABLE table3 (col1 text, col2 int);'
sqlite3 "${DB_PATH}" 'INSERT INTO table3 VALUES (NULL, 1)'
sqlite3 "${DB_PATH}" "INSERT INTO table3 VALUES ('not a null', 2)"
sqlite3 "${DB_PATH}" 'INSERT INTO table3 VALUES (NULL, 3)'
sqlite3 "${DB_PATH}" "INSERT INTO table3 VALUES ('', 4)"

sqlite3 "${DB_PATH}" 'CREATE TABLE table4 (a int, b integer, c tinyint, d smallint, e mediumint, f bigint, g int2, h int8)'
sqlite3 "${DB_PATH}" 'CREATE TABLE table5 (a character(20), b varchar(10), c real, d double, e double precision, f float)'


${CLICKHOUSE_CLIENT} --query="select 'create database engine'";
${CLICKHOUSE_CLIENT} --query="CREATE DATABASE ${CURR_DATABASE} ENGINE = SQLite('${DB_PATH}')"

${CLICKHOUSE_CLIENT} --query="select 'show database tables:'";
${CLICKHOUSE_CLIENT} --query='SHOW TABLES FROM '"${CURR_DATABASE}"';'

${CLICKHOUSE_CLIENT} --query="select 'show creare table:'";
${CLICKHOUSE_CLIENT} --query="SHOW CREATE TABLE ${CURR_DATABASE}.table1;" | sed -r 's/(.*SQLite)(.*)/\1/'
${CLICKHOUSE_CLIENT} --query="SHOW CREATE TABLE ${CURR_DATABASE}.table2;" | sed -r 's/(.*SQLite)(.*)/\1/'

${CLICKHOUSE_CLIENT} --query="select 'describe table:'";
${CLICKHOUSE_CLIENT} --query="DESCRIBE TABLE ${CURR_DATABASE}.table1;"
${CLICKHOUSE_CLIENT} --query="DESCRIBE TABLE ${CURR_DATABASE}.table2;"

${CLICKHOUSE_CLIENT} --query="select 'select *:'";
${CLICKHOUSE_CLIENT} --query="SELECT * FROM ${CURR_DATABASE}.table1 ORDER BY col2"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM ${CURR_DATABASE}.table2 ORDER BY col1"

${CLICKHOUSE_CLIENT} --query="select 'test types'";
${CLICKHOUSE_CLIENT} --query="SHOW CREATE TABLE ${CURR_DATABASE}.table4;" | sed -r 's/(.*SQLite)(.*)/\1/'
${CLICKHOUSE_CLIENT} --query="SHOW CREATE TABLE ${CURR_DATABASE}.table5;" | sed -r 's/(.*SQLite)(.*)/\1/'

${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS ${CURR_DATABASE}"


${CLICKHOUSE_CLIENT} --query="select 'create table engine with table3'";
${CLICKHOUSE_CLIENT} --query='DROP TABLE IF EXISTS sqlite_table3'
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_table3 (col1 String, col2 Int32) ENGINE = SQLite('${DB_PATH}', 'table3')"

${CLICKHOUSE_CLIENT} --query='SHOW CREATE TABLE sqlite_table3;' | sed -r 's/(.*SQLite)(.*)/\1/'
${CLICKHOUSE_CLIENT} --query="INSERT INTO sqlite_table3 VALUES ('line\'6', 6);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO sqlite_table3 VALUES (NULL, 7);"

${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_table3 ORDER BY col2'


${CLICKHOUSE_CLIENT} --query="select 'test table function'";
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'table1') SELECT 'line4', 4"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite('${DB_PATH}', 'table1') ORDER BY col2"


${CLICKHOUSE_CLIENT} --query="select 'test schema inference'";
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_table3_inferred_engine ENGINE = SQLite('${DB_PATH}', 'table3')"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_table3_inferred_function AS sqlite('${DB_PATH}', 'table3')"
${CLICKHOUSE_CLIENT} --query="DESCRIBE TABLE sqlite_table3_inferred_engine;"
${CLICKHOUSE_CLIENT} --query="DESCRIBE TABLE sqlite_table3_inferred_function;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE sqlite_table3_inferred_engine;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE sqlite_table3_inferred_function;"

sqlite3 "${DB_PATH2}" 'DROP TABLE IF EXISTS table1'
sqlite3 "${DB_PATH2}" 'CREATE TABLE table1 (col1 text, col2 smallint);'
sqlite3 "${DB_PATH2}" "INSERT INTO table1 VALUES ('line1', 1), ('line2', 2), ('line3', 3)"

${CLICKHOUSE_CLIENT} --query="select 'test path in clickhouse-local'";
${CLICKHOUSE_LOCAL} --query="SELECT * FROM sqlite('${DB_PATH2}', 'table1') ORDER BY col2"
