#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast-test build does not include the H3 library

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Float32 and BFloat16 inputs must match explicitly converted Float64 inputs.
$CLICKHOUSE_CLIENT --query "SELECT geoToH3(toFloat32(55.71), toFloat64(37.79), toUInt8(15)) = geoToH3(toFloat64(toFloat32(55.71)), toFloat64(37.79), toUInt8(15))"
$CLICKHOUSE_CLIENT --query "SELECT geoToH3(toFloat64(55.71), toFloat32(37.79), toUInt8(15)) = geoToH3(toFloat64(55.71), toFloat64(toFloat32(37.79)), toUInt8(15))"
$CLICKHOUSE_CLIENT --query "SELECT geoToH3(toBFloat16(55.71), toFloat64(37.79), toUInt8(15)) = geoToH3(toFloat64(toBFloat16(55.71)), toFloat64(37.79), toUInt8(15))"

# Legacy argument order uses the same numeric conversions.
$CLICKHOUSE_CLIENT --query "SELECT geoToH3(toFloat32(37.79), toFloat64(55.71), toUInt8(15)) = geoToH3(toFloat64(toFloat32(37.79)), toFloat64(55.71), toUInt8(15)) SETTINGS geotoh3_argument_order = 'lon_lat'"
$CLICKHOUSE_CLIENT --query "SELECT geoToH3(toFloat64(37.79), toFloat32(55.71), toUInt8(15)) = geoToH3(toFloat64(37.79), toFloat64(toFloat32(55.71)), toUInt8(15)) SETTINGS geotoh3_argument_order = 'lon_lat'"

# A stored Float32 alias works and unrelated ALTER statements still succeed.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_04830"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_04830 (f32 Float32, f64 Float64, other String, h UInt64 ALIAS geoToH3(f32, f64, 15)) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_04830 VALUES (55.71, 37.79, 'kept')"
$CLICKHOUSE_CLIENT --query "SELECT other FROM t_04830"
$CLICKHOUSE_CLIENT --query "SELECT h != 0 FROM t_04830"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_04830 MODIFY COMMENT 'unrelated'"
$CLICKHOUSE_CLIENT --query "SELECT other FROM t_04830"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_04830"

# Float64 coordinates remain supported.
$CLICKHOUSE_CLIENT --query "SELECT geoToH3(toFloat64(55.71), toFloat64(37.79), toUInt8(15))"
