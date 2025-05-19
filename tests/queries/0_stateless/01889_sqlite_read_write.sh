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
sqlite3 "${DB_PATH}" "CREATE TABLE \"table6'\" (col1 text, col2 smallint);"
sqlite3 "${DB_PATH}" "INSERT INTO \"table6'\" VALUES ('table6_line1', 1), ('table6_line2', 2), ('table6_line3', 3)"


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

${CLICKHOUSE_CLIENT} --query="select 'populating table4 with integers'";
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'table4') SELECT -9223372036854775808 AS x, x, x, x, x, x, x, x;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'table4') SELECT -2147483648 AS x, x, x, x, x, x, x, x;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'table4') SELECT -32768 AS x, x, x, x, x, x, x, x;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'table4') SELECT -128 AS x, x, x, x, x, x, x, x;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'table4') SELECT 0 AS x, x, x, x, x, x, x, x;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'table4') SELECT 127 AS x, x, x, x, x, x, x, x;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'table4') SELECT 128 AS x, x, x, x, x, x, x, x;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'table4') SELECT 32767 AS x, x, x, x, x, x, x, x;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'table4') SELECT 32768 AS x, x, x, x, x, x, x, x;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'table4') SELECT 2147483647 AS x, x, x, x, x, x, x, x;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'table4') SELECT 2147483648 AS x, x, x, x, x, x, x, x;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'table4') SELECT 9223372036854775807 AS x, x, x, x, x, x, x, x;"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM ${CURR_DATABASE}.table4 ORDER BY a"

${CLICKHOUSE_CLIENT} --query="select 'populating table5 with floats'";
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'table5') SELECT 'a', 'a', 0 AS x, x, x, x;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'table5') SELECT 'b', 'b', 3.141592653589793 AS x, x, x, x;"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM ${CURR_DATABASE}.table5 ORDER BY a"


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
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite('${DB_PATH}', '\\'); select 1 --') ORDER BY col2 -- { serverError SQLITE_ENGINE_ERROR }"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite('${DB_PATH}', 'table6''') ORDER BY col2"


${CLICKHOUSE_CLIENT} --query="select 'test schema inference'";
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_table3_inferred_engine ENGINE = SQLite('${DB_PATH}', 'table3')"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_table3_inferred_function AS sqlite('${DB_PATH}', 'table3')"
${CLICKHOUSE_CLIENT} --query="DESCRIBE TABLE sqlite_table3_inferred_engine;"
${CLICKHOUSE_CLIENT} --query="DESCRIBE TABLE sqlite_table3_inferred_function;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE sqlite_table3_inferred_engine;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE sqlite_table3_inferred_function;"

${CLICKHOUSE_CLIENT} --query="select 'test query syntax with table engine'"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sqlite_query_engine"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_query_engine ENGINE = SQLite('${DB_PATH}', query('SELECT table1.col1, table1.col2, table2.col2 as text FROM table1 JOIN table2 ON table1.col2 = table2.col1'))"
${CLICKHOUSE_CLIENT} --query="DESCRIBE TABLE sqlite_query_engine"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite_query_engine ORDER BY col2"

${CLICKHOUSE_CLIENT} --query="select 'test subquery syntax with table engine'"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sqlite_subquery_engine"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_subquery_engine ENGINE = SQLite('${DB_PATH}', (SELECT col1, col2 FROM table1 WHERE col2 > 1))"
${CLICKHOUSE_CLIENT} --query="DESCRIBE TABLE sqlite_subquery_engine"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite_subquery_engine ORDER BY col2"
${CLICKHOUSE_CLIENT} --query="SELECT col2 FROM sqlite_subquery_engine WHERE col2 < 4 ORDER BY col2"

${CLICKHOUSE_CLIENT} --query="select 'test query function with table function'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite('${DB_PATH}', query('SELECT table1.col1, table1.col2, table2.col2 as text FROM table1 JOIN table2 ON table1.col2 = table2.col1')) ORDER BY col2"

${CLICKHOUSE_CLIENT} --query="select 'test subquery with table function'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite('${DB_PATH}', (SELECT col1, col2 FROM table1 WHERE col2 > 1)) ORDER BY col2"

${CLICKHOUSE_CLIENT} --query="select 'test write to sqlite table while reading from query'"
sqlite3 "${DB_PATH}" 'CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT, price REAL)'
sqlite3 "${DB_PATH}" "INSERT INTO items VALUES (1, 'Item A', 0.5), (2, 'Item B', 0.75), (3, 'Item C', 0.25)"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sqlite_read_query"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_read_query ENGINE = SQLite('${DB_PATH}', query('SELECT name, price, price*1.5 as adjusted_price FROM items'))"
${CLICKHOUSE_CLIENT} --query="DESCRIBE TABLE sqlite_read_query"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite_read_query ORDER BY price"

${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'items') VALUES (4, 'Item D', 0.625)"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite_read_query ORDER BY price"

${CLICKHOUSE_CLIENT} --query="select 'test complex queries with aggregation and schema inference'"
sqlite3 "${DB_PATH}" 'CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY, client_id INTEGER, amount REAL, date TEXT)'
sqlite3 "${DB_PATH}" "INSERT INTO transactions VALUES (1, 101, 0.5, '2023-01-10')"
sqlite3 "${DB_PATH}" "INSERT INTO transactions VALUES (2, 102, 0.25, '2023-01-15')"
sqlite3 "${DB_PATH}" "INSERT INTO transactions VALUES (3, 101, 0.75, '2023-02-05')"
sqlite3 "${DB_PATH}" "INSERT INTO transactions VALUES (4, 103, 0.125, '2023-02-10')"
sqlite3 "${DB_PATH}" "INSERT INTO transactions VALUES (5, 102, 0.375, '2023-03-01')"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sqlite_aggregation"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_aggregation ENGINE = SQLite('${DB_PATH}', query('SELECT client_id, SUM(amount) as total_amount, COUNT(*) as transaction_count, AVG(amount) as avg_amount FROM transactions GROUP BY client_id'))"
${CLICKHOUSE_CLIENT} --query="DESCRIBE TABLE sqlite_aggregation"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite_aggregation ORDER BY client_id"

${CLICKHOUSE_CLIENT} --query="select 'test complex query with table function'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite('${DB_PATH}', query('SELECT client_id, SUM(amount) as total_amount, COUNT(*) as transaction_count, AVG(amount) as avg_amount FROM transactions GROUP BY client_id')) ORDER BY client_id"

${CLICKHOUSE_CLIENT} --query="select 'test creating a table AS sqlite query'"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS item_price_adjusted"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE item_price_adjusted AS sqlite('${DB_PATH}', query('SELECT id, name, price, price*1.5 as adjusted_price FROM items'))"
${CLICKHOUSE_CLIENT} --query="DESCRIBE TABLE item_price_adjusted"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM item_price_adjusted ORDER BY id"

${CLICKHOUSE_CLIENT} --query="select 'test direct queries with conditions'"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sqlite_filtered_data"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_filtered_data ENGINE = SQLite('${DB_PATH}', query('SELECT id, name, price FROM items WHERE price > 0.5'))"
${CLICKHOUSE_CLIENT} --query="DESCRIBE TABLE sqlite_filtered_data"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite_filtered_data ORDER BY id"

${CLICKHOUSE_CLIENT} --query="select 'test filtering with string patterns'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite('${DB_PATH}', query('SELECT * FROM items WHERE price > 0.5 AND name LIKE \"%B%\"')) ORDER BY id"

${CLICKHOUSE_CLIENT} --query="select 'test query with all types and nulls'"
sqlite3 "${DB_PATH}" 'CREATE TABLE IF NOT EXISTS data_types (id INTEGER PRIMARY KEY, int_val INTEGER, real_val REAL, text_val TEXT, null_val INTEGER)'
sqlite3 "${DB_PATH}" "INSERT INTO data_types VALUES (1, 123, 0.5, 'text', NULL)"
sqlite3 "${DB_PATH}" "INSERT INTO data_types VALUES (2, NULL, 0.25, NULL, 0)"
sqlite3 "${DB_PATH}" "INSERT INTO data_types VALUES (3, 456, NULL, 'more text', NULL)"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sqlite_data_types"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_data_types ENGINE = SQLite('${DB_PATH}', query('SELECT * FROM data_types'))"
${CLICKHOUSE_CLIENT} --query="DESCRIBE TABLE sqlite_data_types"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite_data_types ORDER BY id"

${CLICKHOUSE_CLIENT} --query="select 'test union with different types'"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_union_result ENGINE = SQLite('${DB_PATH}', query('SELECT id, int_val FROM data_types UNION ALL SELECT id, real_val FROM data_types'))"
${CLICKHOUSE_CLIENT} --query="DESCRIBE TABLE sqlite_union_result"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite_union_result ORDER BY id, int_val"

${CLICKHOUSE_CLIENT} --query="select 'test empty result inference'"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_empty_result ENGINE = SQLite('${DB_PATH}', query('SELECT * FROM items WHERE price > 100'))"
${CLICKHOUSE_CLIENT} --query="DESCRIBE TABLE sqlite_empty_result"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite_empty_result"

${CLICKHOUSE_CLIENT} --query="select 'clean up test tables'"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sqlite_query_engine"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sqlite_subquery_engine"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sqlite_read_query"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sqlite_aggregation"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS item_price_adjusted"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sqlite_filtered_data"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sqlite_data_types"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sqlite_union_result"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sqlite_empty_result"

${CLICKHOUSE_CLIENT} --query="select 'SQLite truncated int64 value #73141'";
sqlite3 "${DB_PATH}" 'CREATE TABLE t0 (c0 INTEGER);'
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t0 (c0 Int64) ENGINE = MergeTree() ORDER BY tuple();"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE t0 (c0) VALUES (2147483648);"
${CLICKHOUSE_CLIENT} --query="select 'clickhouse'"
${CLICKHOUSE_CLIENT} --query="SELECT t0.c0 FROM t0;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 't0') SELECT c0 FROM t0;"
${CLICKHOUSE_CLIENT} --query="select 'sqlite from clickhouse'"
${CLICKHOUSE_CLIENT} --query="SELECT tx.c0 FROM sqlite('${DB_PATH}', 't0') tx;"
${CLICKHOUSE_CLIENT} --query="select 'sqlite'"
sqlite3 "${DB_PATH}" 'SELECT c0 from t0;'
sqlite3 "${DB_PATH}" 'DROP TABLE t0;'
${CLICKHOUSE_CLIENT} --query="DROP TABLE t0;"

sqlite3 "${DB_PATH2}" 'DROP TABLE IF EXISTS table1'
sqlite3 "${DB_PATH2}" 'CREATE TABLE table1 (col1 text, col2 smallint);'
sqlite3 "${DB_PATH2}" "INSERT INTO table1 VALUES ('line1', 1), ('line2', 2), ('line3', 3)"

${CLICKHOUSE_CLIENT} --query="select 'test path in clickhouse-local'";
${CLICKHOUSE_LOCAL} --query="SELECT * FROM sqlite('${DB_PATH2}', 'table1') ORDER BY col2"
