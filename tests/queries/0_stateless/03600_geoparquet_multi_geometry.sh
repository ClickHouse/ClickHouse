#!/usr/bin/env bash
# Tags: no-fasttest
#       ^ no Parquet support in fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# GeoParquet files with empty geometry_types (spec: "any type") and explicit multiple types
# should be read as the Geometry (Variant) type, not throw an exception.

$CLICKHOUSE_LOCAL -q "SELECT toTypeName(geometry), geometry FROM file('$CURDIR/data_parquet/03600_geoparquet_multi_geometry_empty_types.parquet', Parquet) ORDER BY id;"
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(geometry), geometry FROM file('$CURDIR/data_parquet/03600_geoparquet_multi_geometry_explicit_types.parquet', Parquet) ORDER BY id;"

# Should also work with explicit String type hint (returns raw WKB bytes, no geo parsing)
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(geometry) FROM file('$CURDIR/data_parquet/03600_geoparquet_multi_geometry_empty_types.parquet', Parquet, 'id Int32, geometry String') ORDER BY id;"
