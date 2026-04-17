#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that nullable geo columns are correctly inferred as Nullable(String)
# when using the Arrow-based Parquet reader with geoparquet parser disabled.
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/101845

DATA_FILE="$CUR_DIR/data_parquet/03445_geoparquet_null_point.parquet"

echo "=== Native reader v3: schema inference ==="
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(geometry) FROM file('$DATA_FILE', Parquet) LIMIT 1 SETTINGS input_format_parquet_allow_geoparquet_parser=false, input_format_parquet_use_native_reader_v3=true"

echo "=== Arrow reader: schema inference ==="
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(geometry) FROM file('$DATA_FILE', Parquet) LIMIT 1 SETTINGS input_format_parquet_allow_geoparquet_parser=false, input_format_parquet_use_native_reader_v3=false"

echo "=== Native reader v3: NULL check ==="
$CLICKHOUSE_LOCAL -q "SELECT geometry IS NULL as is_null, name FROM file('$DATA_FILE', Parquet) ORDER BY name SETTINGS input_format_parquet_allow_geoparquet_parser=false, input_format_parquet_use_native_reader_v3=true"

echo "=== Arrow reader: NULL check ==="
$CLICKHOUSE_LOCAL -q "SELECT geometry IS NULL as is_null, name FROM file('$DATA_FILE', Parquet) ORDER BY name SETTINGS input_format_parquet_allow_geoparquet_parser=false, input_format_parquet_use_native_reader_v3=false"
