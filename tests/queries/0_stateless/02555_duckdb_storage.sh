#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
DB_NAME="02555_duckdb_${CLICKHOUSE_DATABASE}"
DB_PATH="${USER_FILES_PATH}/${DB_NAME}.db"

# 1. Database engine

duckdb ${DB_PATH} "CREATE TABLE t1 (a HUGEINT, b BIGINT, c INTEGER, d SMALLINT, e TINYINT)"
duckdb ${DB_PATH} "CREATE TABLE t2 (a UBIGINT, b UINTEGER, c USMALLINT, d UTINYINT)"
duckdb ${DB_PATH} "CREATE TABLE t3 (a REAL, b DOUBLE, c BOOL, d BIT, e INT[])"
duckdb ${DB_PATH} "CREATE TABLE t4 (a VARCHAR, b BLOB, c TIME, d DATE, e TIMESTAMP, f TIMESTAMPTZ, g DECIMAL(10, 5), h UUID)"
duckdb ${DB_PATH} "CREATE TABLE t5 (a INT2, b INT4, c INT8, d FLOAT4)"
duckdb ${DB_PATH} "CREATE TABLE t6 (a INT4, b INT4 NOT NULL, c INT8)"
duckdb ${DB_PATH} "CREATE TABLE 't7''\"\"' (\"a\"\"''\" INT1, \"b''\"\"\" INT1)"

duckdb ${DB_PATH} "INSERT INTO t1 VALUES (170141183460469231731687303715884105727, 9223372036854775807, 2147483647, 32767, 127)"
duckdb ${DB_PATH} "INSERT INTO t1 VALUES (-170141183460469231731687303715884105727, -9223372036854775808, -2147483648, -32768, -128)"
duckdb ${DB_PATH} "INSERT INTO t2 VALUES (18446744073709551615, 4294967295, 65535, 255)"
duckdb ${DB_PATH} "INSERT INTO t3 VALUES (0.123456789, 0.123456789, 1, '0101', [1, 2, 3])"
duckdb ${DB_PATH} "INSERT INTO t3 VALUES (-0.123456789, -0.123456789, 0, '1010', [])"
duckdb ${DB_PATH} "INSERT INTO t4 VALUES ('apple', 'orange', '06:02:03', '1992-03-22', '1992-03-27 01:02:03', '1992-09-20 20:38:48 Europe/Amsterdam', 12345.67890, 'b627362a-2120-49af-84b3-e6da8d3d987c')"
duckdb ${DB_PATH} "INSERT INTO t5 VALUES (1, 2, 3, NULL)"
duckdb ${DB_PATH} "INSERT INTO t5 VALUES (NULL, 2, 3, NULL)"
duckdb ${DB_PATH} "INSERT INTO t5 VALUES (1, NULL, 3, NULL)"
duckdb ${DB_PATH} "INSERT INTO 't7''\"\"' VALUES (1, 1)"

chmod ugo+w "${DB_PATH}"

echo 'SHOW TABLES:'

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB_NAME}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB_NAME} ENGINE = DuckDB('${DB_PATH}')"
${CLICKHOUSE_CLIENT} -q "SHOW TABLES FROM ${DB_NAME}"

echo 'SHOW CREATE TABLE:'

${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${DB_NAME}.t1" | sed -r 's/(.*DuckDB)(.*)/\1/'
${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${DB_NAME}.t2" | sed -r 's/(.*DuckDB)(.*)/\1/'
${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${DB_NAME}.t3" | sed -r 's/(.*DuckDB)(.*)/\1/'
${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${DB_NAME}.t4" | sed -r 's/(.*DuckDB)(.*)/\1/'
${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${DB_NAME}.t5" | sed -r 's/(.*DuckDB)(.*)/\1/'
${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${DB_NAME}.t6" | sed -r 's/(.*DuckDB)(.*)/\1/'
${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${DB_NAME}.\`t7'\"\"\`" | sed -r 's/(.*DuckDB)(.*)/\1/'

echo 'SELECT:'

${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_NAME}.t1"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_NAME}.t2"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_NAME}.t3"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_NAME}.t4"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_NAME}.t5"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_NAME}.t6"
# ${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_NAME}.\`t7'\"\"\`"

echo 'SELECT after INSERT:'

${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB_NAME}.t1 SELECT * FROM ${DB_NAME}.t1"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB_NAME}.t2 SELECT * FROM ${DB_NAME}.t2"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB_NAME}.t3 SELECT * FROM ${DB_NAME}.t3"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB_NAME}.t4 SELECT * FROM ${DB_NAME}.t4"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB_NAME}.t5 SELECT * FROM ${DB_NAME}.t5"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB_NAME}.t6 SELECT * FROM ${DB_NAME}.t6"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_NAME}.t1"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_NAME}.t2"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_NAME}.t3"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_NAME}.t4"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_NAME}.t5"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_NAME}.t6"

echo 'SELECT after large INSERT:'

${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB_NAME}.t6 SELECT number, rand32(), rand64() FROM numbers(100)"
${CLICKHOUSE_CLIENT} -q "SELECT uniqExact(a), min(a), max(a), count(b) FROM ${DB_NAME}.t6"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB_NAME}"

# 2. Table function

echo 'SELECT from table function:'

${CLICKHOUSE_CLIENT} -q "SELECT * FROM duckdb('${DB_PATH}', 't1')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM duckdb('${DB_PATH}', 't2')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM duckdb('${DB_PATH}', 't3')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM duckdb('${DB_PATH}', 't4')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM duckdb('${DB_PATH}', 't5')"
${CLICKHOUSE_CLIENT} -q "SELECT uniqExact(a), min(a), max(a), count(b) FROM duckdb('${DB_PATH}', 't6')"

echo 'SELECT from table function after insert:'

${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION duckdb('${DB_PATH}', 't1') SELECT * FROM duckdb('${DB_PATH}', 't1')"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION duckdb('${DB_PATH}', 't2') SELECT * FROM duckdb('${DB_PATH}', 't2')"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION duckdb('${DB_PATH}', 't3') SELECT * FROM duckdb('${DB_PATH}', 't3')"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION duckdb('${DB_PATH}', 't4') SELECT * FROM duckdb('${DB_PATH}', 't4')"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION duckdb('${DB_PATH}', 't5') SELECT * FROM duckdb('${DB_PATH}', 't5')"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION duckdb('${DB_PATH}', 't6') SELECT * FROM duckdb('${DB_PATH}', 't6')"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM duckdb('${DB_PATH}', 't1')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM duckdb('${DB_PATH}', 't2')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM duckdb('${DB_PATH}', 't3')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM duckdb('${DB_PATH}', 't4')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM duckdb('${DB_PATH}', 't5')"
${CLICKHOUSE_CLIENT} -q "SELECT uniqExact(a), min(a), max(a), count(b) FROM duckdb('${DB_PATH}', 't6')"

# 3. Table engine

echo 'SELECT from table with DuckDB engine:'

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t1 (a Int128, b Int64, c Int32, d Int16, e Int8) ENGINE = DuckDB('${DB_PATH}', 't1')"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t2 (a UInt64, b UInt32, c UInt16, d UInt8) ENGINE = DuckDB('${DB_PATH}', 't2')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM t1"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM t2"

echo 'SELECT from table with DuckDB engine after insert:'

${CLICKHOUSE_CLIENT} -q "INSERT INTO t2 SELECT * FROM t2"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM t2"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t2"

rm ${DB_PATH}
