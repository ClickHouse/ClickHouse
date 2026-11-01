#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast-test build does not include the H3 library

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

extract_error() {
    grep -m1 -oE "Illegal type [A-Za-z0-9]+ of argument [0-9]+ of function geoToH3"
}

# Default order: geoToH3(latitude, longitude, resolution)
$CLICKHOUSE_CLIENT --query "SELECT geoToH3(toFloat32(55.71), toFloat64(37.79), toUInt8(15))" 2>&1 | extract_error
$CLICKHOUSE_CLIENT --query "SELECT geoToH3(toFloat64(55.71), toFloat32(37.79), toUInt8(15))" 2>&1 | extract_error
$CLICKHOUSE_CLIENT --query "SELECT geoToH3(toBFloat16(55.71), toFloat64(37.79), toUInt8(15))" 2>&1 | extract_error

# Legacy order: geoToH3(longitude, latitude, resolution)
$CLICKHOUSE_CLIENT --query "SET geotoh3_argument_order = 'lon_lat'; SELECT geoToH3(toFloat32(37.79), toFloat64(55.71), toUInt8(15))" 2>&1 | extract_error
$CLICKHOUSE_CLIENT --query "SET geotoh3_argument_order = 'lon_lat'; SELECT geoToH3(toFloat64(37.79), toFloat32(55.71), toUInt8(15))" 2>&1 | extract_error

# A stored expression over Float32 coordinates keeps loading: unrelated columns stay readable
# and an unrelated ALTER still succeeds.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_04830"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_04830 (f32 Float32, f64 Float64, other String, h UInt64 ALIAS geoToH3(f32, f64, 15)) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_04830 VALUES (55.71, 37.79, 'kept')"
$CLICKHOUSE_CLIENT --query "SELECT other FROM t_04830"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_04830 MODIFY COMMENT 'unrelated'"
$CLICKHOUSE_CLIENT --query "SELECT other FROM t_04830"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_04830"

# Float64 coordinates are accepted
$CLICKHOUSE_CLIENT --query "SELECT geoToH3(toFloat64(55.71), toFloat64(37.79), toUInt8(15))"
