#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/data_sqlite/db1


${CLICKHOUSE_CLIENT} --query='DROP DATABASE IF EXISTS sqlite_database'

${CLICKHOUSE_CLIENT} --query="select 'create database engine'";
${CLICKHOUSE_CLIENT} --query="CREATE DATABASE sqlite_database ENGINE = SQLite('${DATA_FILE}')"

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

${CLICKHOUSE_CLIENT} --query="select 'test insert:'";
${CLICKHOUSE_CLIENT} --query="INSERT INTO  sqlite_database.table1 VALUES ('line4', 4);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO  sqlite_database.table2 VALUES (4, 'text4');"
${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_database.table1 WHERE col2 > 3 ORDER BY col2;'
${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_database.table2 WHERE col1 > 3 ORDER BY col1;'

${CLICKHOUSE_CLIENT} --query="select 'test NULLs:'";
sqlite3 data_sqlite/db1 'INSERT INTO table3 VALUES (NULL, 2)'
${CLICKHOUSE_CLIENT} --query="INSERT INTO  sqlite_database.table3 VALUES (NULL, 3);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO  sqlite_database.table3 VALUES (NULL, 4);"
${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_database.table3 ORDER BY col2;'

sqlite3 data_sqlite/db1 'DELETE FROM table1 WHERE col2 > 3'
sqlite3 data_sqlite/db1 'DELETE FROM table2 WHERE col1 > 3'
sqlite3 data_sqlite/db1 'DELETE FROM table3 WHERE col2 > 1'

${CLICKHOUSE_CLIENT} --query="select 'after delete:'";
${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_database.table1 WHERE col2 > 3 ORDER BY col2;'
${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_database.table2 WHERE col1 > 3 ORDER BY col1;'

${CLICKHOUSE_CLIENT} --query="select 'detach'";
${CLICKHOUSE_CLIENT} --query='DETACH DATABASE sqlite_database;'
${CLICKHOUSE_CLIENT} --query='ATTACH DATABASE sqlite_database;'

${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_database.table1 ORDER BY col2'
${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_database.table2 ORDER BY col1;'

${CLICKHOUSE_CLIENT} --query='DROP DATABASE IF EXISTS sqlite_database;'

${CLICKHOUSE_CLIENT} --query="select 'create table engine with table1'";
${CLICKHOUSE_CLIENT} --query='DROP TABLE IF EXISTS sqlite_table1'
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_table1 (col1 String, col2 Int32) ENGINE = SQLite('${DATA_FILE}', 'table1')"
${CLICKHOUSE_CLIENT} --query='SHOW CREATE TABLE sqlite_table1;' | sed -r 's/(.*SQLite)(.*)/\1/'
${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_table1 ORDER BY col2'
${CLICKHOUSE_CLIENT} --query="INSERT INTO sqlite_table1 VALUES ('test1', 12);"
${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_table1 WHERE col2 = 12'
sqlite3 data_sqlite/db1 'DELETE FROM table1 WHERE col2 = 12'

${CLICKHOUSE_CLIENT} --query="select 'create table engine with table2'";
${CLICKHOUSE_CLIENT} --query='DROP TABLE IF EXISTS sqlite_table2'
${CLICKHOUSE_CLIENT} --query="CREATE TABLE sqlite_table2 (col1 Int32, col2 String) ENGINE = SQLite('${DATA_FILE}', 'table2')"
${CLICKHOUSE_CLIENT} --query='SHOW CREATE TABLE sqlite_table2;' | sed -r 's/(.*SQLite)(.*)/\1/'
${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_table2 ORDER BY col1'
${CLICKHOUSE_CLIENT} --query="INSERT INTO sqlite_table2 VALUES (12, 'test2');"
${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_table2 WHERE col1 = 12'
sqlite3 data_sqlite/db1 'DELETE FROM table2 WHERE col1 = 12'

