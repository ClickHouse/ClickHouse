#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/03445_geoparquet_wkb.parquet', Parquet)"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/03445_geoparquet_wkt.parquet', Parquet)"
