#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/test_${CLICKHOUSE_DATABASE}_sqlite_datetime.db"

rm -f "${DB_PATH}"

# Test DateTime type - SQLite stores TEXT, ClickHouse reads as DateTime
sqlite3 "${DB_PATH}" 'CREATE TABLE tx_datetime (c0 TEXT);'
sqlite3 "${DB_PATH}" "INSERT INTO tx_datetime VALUES ('2024-01-01 12:00:00');"

# Test Date type
sqlite3 "${DB_PATH}" 'CREATE TABLE tx_date (c0 TEXT);'
sqlite3 "${DB_PATH}" "INSERT INTO tx_date VALUES ('2024-01-15');"

# Test Date32 type
sqlite3 "${DB_PATH}" 'CREATE TABLE tx_date32 (c0 TEXT);'
sqlite3 "${DB_PATH}" "INSERT INTO tx_date32 VALUES ('1900-01-01');"

# Test DateTime64 type
sqlite3 "${DB_PATH}" 'CREATE TABLE tx_datetime64 (c0 TEXT);'
sqlite3 "${DB_PATH}" "INSERT INTO tx_datetime64 VALUES ('2024-01-01 12:00:00.123456');"

# Test UUID type
sqlite3 "${DB_PATH}" 'CREATE TABLE tx_uuid (c0 TEXT);'
sqlite3 "${DB_PATH}" "INSERT INTO tx_uuid VALUES ('550e8400-e29b-41d4-a716-446655440000');"

# Test Enum8 type
sqlite3 "${DB_PATH}" 'CREATE TABLE tx_enum8 (c0 TEXT);'
sqlite3 "${DB_PATH}" "INSERT INTO tx_enum8 VALUES ('hello');"

# Test Enum16 type
sqlite3 "${DB_PATH}" 'CREATE TABLE tx_enum16 (c0 TEXT);'
sqlite3 "${DB_PATH}" "INSERT INTO tx_enum16 VALUES ('world');"

# Test Decimal types
sqlite3 "${DB_PATH}" 'CREATE TABLE tx_decimal32 (c0 TEXT);'
sqlite3 "${DB_PATH}" "INSERT INTO tx_decimal32 VALUES ('123.45');"

sqlite3 "${DB_PATH}" 'CREATE TABLE tx_decimal64 (c0 TEXT);'
sqlite3 "${DB_PATH}" "INSERT INTO tx_decimal64 VALUES ('12345678.901234');"

sqlite3 "${DB_PATH}" 'CREATE TABLE tx_decimal128 (c0 TEXT);'
sqlite3 "${DB_PATH}" "INSERT INTO tx_decimal128 VALUES ('123456789012345678.901234567890');"

sqlite3 "${DB_PATH}" 'CREATE TABLE tx_decimal256 (c0 TEXT);'
sqlite3 "${DB_PATH}" "INSERT INTO tx_decimal256 VALUES ('12345678901234567890123456789012345678.901234567890');"

# Test FixedString type
sqlite3 "${DB_PATH}" 'CREATE TABLE tx_fixedstring (c0 TEXT);'
sqlite3 "${DB_PATH}" "INSERT INTO tx_fixedstring VALUES ('abc');"

${CLICKHOUSE_LOCAL} --query="
CREATE DATABASE tmpdb ENGINE = SQLite('${DB_PATH}');

-- Test DateTime: this triggers the bug - Bad cast from ColumnVector<unsigned int> to ColumnString
CREATE TABLE t_datetime (c0 DateTime) ENGINE = SQLite('${DB_PATH}', 'tx_datetime');
SELECT * FROM t_datetime;

-- Test Date
CREATE TABLE t_date (c0 Date) ENGINE = SQLite('${DB_PATH}', 'tx_date');
SELECT * FROM t_date;

-- Test Date32
CREATE TABLE t_date32 (c0 Date32) ENGINE = SQLite('${DB_PATH}', 'tx_date32');
SELECT * FROM t_date32;

-- Test DateTime64
CREATE TABLE t_datetime64 (c0 DateTime64(6)) ENGINE = SQLite('${DB_PATH}', 'tx_datetime64');
SELECT * FROM t_datetime64;

-- Test UUID
CREATE TABLE t_uuid (c0 UUID) ENGINE = SQLite('${DB_PATH}', 'tx_uuid');
SELECT * FROM t_uuid;

-- Test Enum8
CREATE TABLE t_enum8 (c0 Enum8('hello' = 1, 'world' = 2)) ENGINE = SQLite('${DB_PATH}', 'tx_enum8');
SELECT * FROM t_enum8;

-- Test Enum16
CREATE TABLE t_enum16 (c0 Enum16('hello' = 1000, 'world' = 2000)) ENGINE = SQLite('${DB_PATH}', 'tx_enum16');
SELECT * FROM t_enum16;

-- Test Decimal32
CREATE TABLE t_decimal32 (c0 Decimal32(2)) ENGINE = SQLite('${DB_PATH}', 'tx_decimal32');
SELECT * FROM t_decimal32;

-- Test Decimal64
CREATE TABLE t_decimal64 (c0 Decimal64(6)) ENGINE = SQLite('${DB_PATH}', 'tx_decimal64');
SELECT * FROM t_decimal64;

-- Test Decimal128
CREATE TABLE t_decimal128 (c0 Decimal128(12)) ENGINE = SQLite('${DB_PATH}', 'tx_decimal128');
SELECT * FROM t_decimal128;

-- Test Decimal256
CREATE TABLE t_decimal256 (c0 Decimal256(12)) ENGINE = SQLite('${DB_PATH}', 'tx_decimal256');
SELECT * FROM t_decimal256;

-- Test FixedString
CREATE TABLE t_fixedstring (c0 FixedString(10)) ENGINE = SQLite('${DB_PATH}', 'tx_fixedstring');
SELECT * FROM t_fixedstring;
"

rm -f "${DB_PATH}"
