#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE1=$CUR_DIR/data_sqlite/db1
DATA_FILE2=$CUR_DIR/db2

${CLICKHOUSE_CLIENT} --query='DROP DATABASE IF EXISTS sqlite_database'

${CLICKHOUSE_CLIENT} --query="select 'create database engine'";
${CLICKHOUSE_CLIENT} --query="CREATE DATABASE sqlite_database ENGINE = SQLite('${DATA_FILE1}')"

${CLICKHOUSE_CLIENT} --query="select 'show database tables:'";
${CLICKHOUSE_CLIENT} --query='SHOW TABLES FROM sqlite_database;'

${CLICKHOUSE_CLIENT} --query="select 'describe table:'";
${CLICKHOUSE_CLIENT} --query='DESCRIBE TABLE sqlite_database.table1;'
${CLICKHOUSE_CLIENT} --query='DESCRIBE TABLE sqlite_database.table2;'

${CLICKHOUSE_CLIENT} --query="select 'describe table:'";
${CLICKHOUSE_CLIENT} --query='SHOW CREATE TABLE sqlite_database.table1;' | sed -r 's/(.*SQLite)(.*)/\1/'
${CLICKHOUSE_CLIENT} --query='SHOW CREATE TABLE sqlite_database.table2;' | sed -r 's/(.*SQLite)(.*)/\1/'

${CLICKHOUSE_CLIENT} --query="select 'select *:'";
${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_database.table1 ORDER BY col2'
${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_database.table2 ORDER BY col1;'

sqlite3 $CUR_DIR/db2 'DROP TABLE IF EXISTS table3'
sqlite3 $CUR_DIR/db2 'CREATE TABLE table3 (col1 text, col2 int)'
sqlite3 $CUR_DIR/db2 'INSERT INTO table3 VALUES (NULL, 1)'
sqlite3 $CUR_DIR/db2 "INSERT INTO table3 VALUES ('not a null', 2)"
sqlite3 $CUR_DIR/db2 'INSERT INTO table3 VALUES (NULL, 3)'
sqlite3 $CUR_DIR/db2 "INSERT INTO table3 VALUES ('', 4)"

${CLICKHOUSE_CLIENT} --query='DROP DATABASE IF EXISTS sqlite_database_2'
${CLICKHOUSE_CLIENT} --query="CREATE DATABASE sqlite_database_2 ENGINE = SQLite('${DATA_FILE2}')"
# Do not run these, bacuase requires permissions in ci for write access to the directory of the created file and chmod does not help.
# ${CLICKHOUSE_CLIENT} --query="INSERT INTO  sqlite_database_2.table3 VALUES (NULL, 3);"
# ${CLICKHOUSE_CLIENT} --query="INSERT INTO  sqlite_database_2.table3 VALUES (NULL, 4);"
# ${CLICKHOUSE_CLIENT} --query="INSERT INTO  sqlite_database_2.table3 VALUES ('line5', 5);"
${CLICKHOUSE_CLIENT} --query="select 'test NULLs:'";
${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_database_2.table3 ORDER BY col2;'

${CLICKHOUSE_CLIENT} --query="select 'detach'";
${CLICKHOUSE_CLIENT} --query='DETACH DATABASE sqlite_database;'
${CLICKHOUSE_CLIENT} --query='ATTACH DATABASE sqlite_database;'

${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_database.table1 ORDER BY col2'
${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_database.table2 ORDER BY col1;'

${CLICKHOUSE_CLIENT} --query='DROP DATABASE IF EXISTS sqlite_database;'

${CLICKHOUSE_CLIENT} --query="select 'create table engine with table3'";
${CLICKHOUSE_CLIENT} --query='DROP TABLE IF EXISTS sqlite_table3'
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_table3 (col1 String, col2 Int32) ENGINE = SQLite('${DATA_FILE2}', 'table3')"
${CLICKHOUSE_CLIENT} --query='SHOW CREATE TABLE sqlite_table3;' | sed -r 's/(.*SQLite)(.*)/\1/'
# Do not run these, bacuase requires permissions in ci for write access to the directory of the created file and chmod does not help.
# ${CLICKHOUSE_CLIENT} --query="INSERT INTO  sqlite_table3 VALUES ('line6', 6);"
# ${CLICKHOUSE_CLIENT} --query="INSERT INTO  sqlite_table3 VALUES (NULL, 7);"
${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_table3 ORDER BY col2'

sqlite3 $CUR_DIR/db2 'DROP TABLE IF EXISTS table4'
sqlite3 $CUR_DIR/db2 'CREATE TABLE table4 (a int, b integer, c tinyint, d smallint, e mediumint, bigint, int2, int8)'
${CLICKHOUSE_CLIENT} --query="select 'test types'";
${CLICKHOUSE_CLIENT} --query='SHOW CREATE TABLE sqlite_database_2.table4;' | sed -r 's/(.*SQLite)(.*)/\1/'
sqlite3 $CUR_DIR/db2 'CREATE TABLE table5 (a character(20), b varchar(10), c real, d double, e double precision, f float)'
${CLICKHOUSE_CLIENT} --query='SHOW CREATE TABLE sqlite_database_2.table5;' | sed -r 's/(.*SQLite)(.*)/\1/'

${CLICKHOUSE_CLIENT} --query="select 'test table function'";
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite('${DATA_FILE1}', 'table1') ORDER BY col2"

rm ${DATA_FILE2}
