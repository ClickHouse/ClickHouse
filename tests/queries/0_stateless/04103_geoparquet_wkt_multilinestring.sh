#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_parquet/04103_geoparquet_wkt_multilinestring.parquet', Parquet) ORDER BY id"
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(geom) FROM file('$CUR_DIR/data_parquet/04103_geoparquet_wkt_multilinestring.parquet', Parquet) LIMIT 1"
