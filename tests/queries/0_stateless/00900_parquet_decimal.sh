#!/usr/bin/env bash

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS decimal;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS decimal2;"

# Simple small values
${CLICKHOUSE_CLIENT} --query="CREATE TABLE IF NOT EXISTS decimal  (a DECIMAL(9,0), b DECIMAL(18,0), c DECIMAL(38,0), d DECIMAL(9, 9), e DECIMAL(18, 18), f DECIMAL(38, 38), g Decimal(9, 5), h decimal(18, 9), i deciMAL(38, 18), j DECIMAL(1,0)) ENGINE = Memory;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE IF NOT EXISTS decimal2 AS decimal ENGINE = Memory;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j) VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0);"
#${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j) VALUES (1, 1, 1, 0.1, 0.1, 1, 1, 1, 1, 1);"
#${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j) VALUES (10, 10, 10, 0.1, 0.1, 0.1, 10, 10, 10, 10);"
#${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j) VALUES (-100, -100, -100, -0.1, -0.1, -0.1, -100, -100, -100, -100);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c) VALUES (1, 1, 1);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c) VALUES (10, 10, 10);"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d, e, f, g, h, i, j;" > "${CLICKHOUSE_TMP}"/parquet_decimal0_1.dump
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d, e, f, g, h, i, j FORMAT Parquet;" > "${CLICKHOUSE_TMP}"/parquet_decimal0.parquet
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d, e, f, g, h, i, j FORMAT Parquet;" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal2 FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal2 ORDER BY a, b, c, d, e, f, g, h, i, j;" > "${CLICKHOUSE_TMP}"/parquet_decimal0_2.dump
echo diff0:
diff "${CLICKHOUSE_TMP}"/parquet_decimal0_1.dump "${CLICKHOUSE_TMP}"/parquet_decimal0_2.dump
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS decimal;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS decimal2;"


${CLICKHOUSE_CLIENT} --query="CREATE TABLE IF NOT EXISTS decimal  ( a DECIMAL(9,0), b DECIMAL(18,0), c DECIMAL(38,0), d DECIMAL(9, 9), e DECIMAL(18, 18), f DECIMAL(38, 38), g Decimal(9, 5), h decimal(18, 9), i deciMAL(38, 18), j DECIMAL(1,0)) ENGINE = Memory;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE IF NOT EXISTS decimal2 AS decimal ENGINE = Memory;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, d, g) VALUES (999999999, 999999999999999999, 0.999999999, 9999.99999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, d, g) VALUES (-999999999, -999999999999999999, -0.999999999, -9999.99999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (c) VALUES (99999999999999999999999999999999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (c) VALUES (-99999999999999999999999999999999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (f) VALUES (0.99999999999999999999999999999999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (f) VALUES (-0.99999999999999999999999999999999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (e, h) VALUES (0.999999999999999999, 999999999.999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (e, h) VALUES (-0.999999999999999999, -999999999.999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (i) VALUES (99999999999999999999.999999999999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (i) VALUES (-99999999999999999999.999999999999999999);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c, d, g, j, h) VALUES (1, 1, 1, 0.000000001, 0.00001, 1, 0.000000001);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c, d, g, j, h) VALUES (-1, -1, -1, -0.000000001, -0.00001, -1, -0.000000001);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (e, f) VALUES (0.000000000000000001, 0.00000000000000000000000000000000000001);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (e, f) VALUES (-0.000000000000000001, -0.00000000000000000000000000000000000001);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (i) VALUES (0.000000000000000001);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (i) VALUES (-0.000000000000000001);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j) VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j) VALUES (-0, -0, -0, -0, -0, -0, -0, -0, -0, -0);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j) VALUES (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j) VALUES (-0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, g) VALUES ('42.00000', 42.0000000000000000000000000000000, '0.999990');"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c, d, e, f) VALUES ('0.9e9', '0.9e18', '0.9e38', '9e-9', '9e-18', '9e-38');"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c, d, e, f) VALUES ('-0.9e9', '-0.9e18', '-0.9e38', '-9e-9', '-9e-18', '-9e-38');"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d, e, f, g, h, i, j;" > "${CLICKHOUSE_TMP}"/parquet_decimal1_1.dump
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d, e, f, g, h, i, j FORMAT Parquet;" > "${CLICKHOUSE_TMP}"/parquet_decimal1.parquet
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d, e, f, g, h, i, j FORMAT Parquet;" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal2 FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal2 ORDER BY a, b, c, d, e, f, g, h, i, j;" > "${CLICKHOUSE_TMP}"/parquet_decimal1_2.dump
echo diff1:
diff "${CLICKHOUSE_TMP}"/parquet_decimal1_1.dump "${CLICKHOUSE_TMP}"/parquet_decimal1_2.dump
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS decimal;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS decimal2;"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE IF NOT EXISTS decimal  (a DECIMAL(9,0), b DECIMAL(18,0), c DECIMAL(38,0), d DECIMAL(9, 9), e Decimal64(18), f Decimal128(38), g Decimal32(5), h Decimal64(9), i Decimal128(18), j dec(4,2)) ENGINE = Memory;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE IF NOT EXISTS decimal2 AS decimal ENGINE = Memory;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j) VALUES (42, 42, 42, 0.42, 0.42, 0.42, 42.42, 42.42, 42.42, 42.42);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j) VALUES (-42, -42, -42, -0.42, -0.42, -0.42, -42.42, -42.42, -42.42, -42.42);"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d, e, f, g, h, i, j;" > "${CLICKHOUSE_TMP}"/parquet_decimal2_1.dump
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d, e, f, g, h, i, j FORMAT Parquet;" > "${CLICKHOUSE_TMP}"/parquet_decimal2.parquet
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d, e, f, g, h, i, j FORMAT Parquet;" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal2 FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal2 ORDER BY a, b, c, d, e, f, g, h, i, j;" > "${CLICKHOUSE_TMP}"/parquet_decimal2_2.dump
echo diff2:
diff "${CLICKHOUSE_TMP}"/parquet_decimal2_1.dump "${CLICKHOUSE_TMP}"/parquet_decimal2_2.dump
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS decimal;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS decimal2;"


${CLICKHOUSE_CLIENT} --query="CREATE TABLE IF NOT EXISTS decimal  (a Nullable(DECIMAL(9,0)), b Nullable(DECIMAL(18,0)), c Nullable(DECIMAL(38,0)), d Nullable(DECIMAL(9,0))) ENGINE = Memory;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE IF NOT EXISTS decimal2 AS decimal ENGINE = Memory;"
# Empty table test
# throws No data to insert
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d  FORMAT Parquet;" > "${CLICKHOUSE_TMP}"/parquet_decimal3_1.parquet
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d FORMAT Parquet;" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal2 FORMAT Parquet" 2> /dev/null
echo nothing:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal2 ORDER BY a, b, c, d;"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE decimal2;"

${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal VALUES (Null, Null, Null, Null)"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d FORMAT Parquet;" > "${CLICKHOUSE_TMP}"/parquet_decimal3_2.parquet
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d FORMAT Parquet;" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal2 FORMAT Parquet"
echo nulls:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal2 ORDER BY a, b, c, d;"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE decimal2;"

${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal VALUES (1, Null, Null, Null)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal VALUES (Null, 1, Null, Null)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal VALUES (Null, Null, 1, Null)"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d FORMAT Parquet;" > "${CLICKHOUSE_TMP}"/parquet_decimal3_3.parquet
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d FORMAT Parquet;" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO decimal2 FORMAT Parquet"

echo full orig:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal ORDER BY a, b, c, d;"
echo full inserted:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal2 ORDER BY a, b, c, d;"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal2 ORDER BY a, b, c, d;" > "${CLICKHOUSE_TMP}"/parquet_decimal3_1.dump
${CLICKHOUSE_CLIENT} --query="SELECT * FROM decimal2 ORDER BY a, b, c, d;" > "${CLICKHOUSE_TMP}"/parquet_decimal3_2.dump

echo diff3:
diff "${CLICKHOUSE_TMP}"/parquet_decimal3_1.dump "${CLICKHOUSE_TMP}"/parquet_decimal3_2.dump
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS decimal;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS decimal2;"
