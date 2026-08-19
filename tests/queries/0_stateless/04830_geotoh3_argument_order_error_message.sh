#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast-test build does not include the H3 library

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

extract_error() {
    grep -m1 -oE "as [0-9]+(st|nd|rd|th) argument '[a-z]+' to function 'geoToH3'\. Expected: [A-Za-z0-9]+, got: [A-Za-z0-9]+"
}

# Default order: geoToH3(latitude, longitude, resolution)
$CLICKHOUSE_CLIENT --query "SELECT geoToH3(toFloat32(55.71), toFloat64(37.79), toUInt8(15))" 2>&1 | extract_error
$CLICKHOUSE_CLIENT --query "SELECT geoToH3(toFloat64(55.71), toFloat32(37.79), toUInt8(15))" 2>&1 | extract_error
$CLICKHOUSE_CLIENT --query "SELECT geoToH3(toBFloat16(55.71), toFloat64(37.79), toUInt8(15))" 2>&1 | extract_error

# Legacy order: geoToH3(longitude, latitude, resolution)
$CLICKHOUSE_CLIENT --query "SET geotoh3_argument_order = 'lon_lat'; SELECT geoToH3(toFloat32(37.79), toFloat64(55.71), toUInt8(15))" 2>&1 | extract_error
$CLICKHOUSE_CLIENT --query "SET geotoh3_argument_order = 'lon_lat'; SELECT geoToH3(toFloat64(37.79), toFloat32(55.71), toUInt8(15))" 2>&1 | extract_error

# Float64 coordinates are accepted
$CLICKHOUSE_CLIENT --query "SELECT geoToH3(toFloat64(55.71), toFloat64(37.79), toUInt8(15))"
