#!/usr/bin/env bash

#
# Load all possible .parquet files found in submodules.
# TODO: Add more files.
#

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CUR_DIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.decimal;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.decimal2;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE IF NOT EXISTS test.decimal( a DECIMAL(9,0), b DECIMAL(18,0), c DECIMAL(38,0), d DECIMAL(9, 9), e DECIMAL(18, 18), f DECIMAL(38, 38), g Decimal(9, 5), h decimal(18, 9), i deciMAL(38, 18), j DECIMAL(1,0)) ENGINE = Memory;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE IF NOT EXISTS test.decimal2( a DECIMAL(9,0), b DECIMAL(18,0), c DECIMAL(38,0), d DECIMAL(9, 9), e DECIMAL(18, 18), f DECIMAL(38, 38), g Decimal(9, 5), h decimal(18, 9), i deciMAL(38, 18), j DECIMAL(1,0)) ENGINE = Memory;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (a, b, d, g) VALUES (999999999, 999999999999999999, 0.999999999, 9999.99999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (a, b, d, g) VALUES (-999999999, -999999999999999999, -0.999999999, -9999.99999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (c) VALUES (99999999999999999999999999999999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (c) VALUES (-99999999999999999999999999999999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (f) VALUES (0.99999999999999999999999999999999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (f) VALUES (-0.99999999999999999999999999999999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (e, h) VALUES (0.999999999999999999, 999999999.999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (e, h) VALUES (-0.999999999999999999, -999999999.999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (i) VALUES (99999999999999999999.999999999999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (i) VALUES (-99999999999999999999.999999999999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (a, b, c, d, g, j, h) VALUES (1, 1, 1, 0.000000001, 0.00001, 1, 0.000000001);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (a, b, c, d, g, j, h) VALUES (-1, -1, -1, -0.000000001, -0.00001, -1, -0.000000001);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (e, f) VALUES (0.000000000000000001, 0.00000000000000000000000000000000000001);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (e, f) VALUES (-0.000000000000000001, -0.00000000000000000000000000000000000001);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (i) VALUES (0.000000000000000001);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (i) VALUES (-0.000000000000000001);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (a, b, c, d, e, f, g, h, i, j) VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (a, b, c, d, e, f, g, h, i, j) VALUES (-0, -0, -0, -0, -0, -0, -0, -0, -0, -0);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (a, b, c, d, e, f, g, h, i, j) VALUES (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (a, b, c, d, e, f, g, h, i, j) VALUES (-0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (a, b, g) VALUES ('42.00000', 42.0000000000000000000000000000000, '0.999990');"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (a, b, c, d, e, f) VALUES ('0.9e9', '0.9e18', '0.9e38', '9e-9', '9e-18', '9e-38');"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (a, b, c, d, e, f) VALUES ('-0.9e9', '-0.9e18', '-0.9e38', '-9e-9', '-9e-18', '-9e-38');"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.decimal ORDER BY a, b, c, d, e, f, g, h, i, j FORMAT Parquet;" > ${CLICKHOUSE_TMP}/parquet_decimal1.parquet
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.decimal ORDER BY a, b, c, d, e, f, g, h, i, j FORMAT Parquet;" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal2 FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.decimal2 ORDER BY a, b, c, d, e, f, g, h, i, j;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.decimal;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.decimal2;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE IF NOT EXISTS test.decimal (a DECIMAL(9,0), b DECIMAL(18,0), c DECIMAL(38,0), d DECIMAL(9, 9), e Decimal64(18), f Decimal128(38), g Decimal32(5), h Decimal64(9), i Decimal128(18), j dec(4,2)) ENGINE = Memory;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE IF NOT EXISTS test.decimal2 (a DECIMAL(9,0), b DECIMAL(18,0), c DECIMAL(38,0), d DECIMAL(9, 9), e Decimal64(18), f Decimal128(38), g Decimal32(5), h Decimal64(9), i Decimal128(18), j dec(4,2)) ENGINE = Memory;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (a, b, c, d, e, f, g, h, i, j) VALUES (42, 42, 42, 0.42, 0.42, 0.42, 42.42, 42.42, 42.42, 42.42);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal (a, b, c, d, e, f, g, h, i, j) VALUES (-42, -42, -42, -0.42, -0.42, -0.42, -42.42, -42.42, -42.42, -42.42);"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.decimal ORDER BY a, b, c, d, e, f, g, h, i, j FORMAT Parquet;" > ${CLICKHOUSE_TMP}/parquet_decimal2.parquet
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.decimal ORDER BY a, b, c, d, e, f, g, h, i, j FORMAT Parquet;" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.decimal2 FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.decimal2 ORDER BY a, b, c, d, e, f, g, h, i, j;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.decimal;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.decimal2;"
