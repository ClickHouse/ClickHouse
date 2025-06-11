#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/03445_geoparquet_wkb.parquet', Parquet)"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/03445_geoparquet_wkt.parquet', Parquet)"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/03445_geoparquet_null_point.parquet', Parquet)"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/03445_geoparquet_null_linestring.parquet', Parquet)"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/03445_geoparquet_null_polygon.parquet', Parquet)"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/03445_geoparquet_wkb.parquet', Parquet) SETTINGS input_format_parquet_allow_geoparquet_parser=false;"
$CLICKHOUSE_LOCAL -q "select toTypeName(point_id), toTypeName(line_id), toTypeName(polygon_id), toTypeName(multilines_id), toTypeName(multipolygons_id) from file('$CURDIR/data_parquet/03445_geoparquet_wkb.parquet', Parquet);"
