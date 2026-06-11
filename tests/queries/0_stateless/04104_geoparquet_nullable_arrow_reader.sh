#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that nullable geo columns are correctly inferred as Nullable(String)
# when reading Arrow format with geoparquet parser disabled.
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/101845
#
# The fix is in ArrowColumnToCHColumn which is shared between Parquet (Arrow
# reader), Arrow, and ORC formats.  We test via Arrow IPC to exercise the code
# path directly.

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

# Create an Arrow IPC file with geo metadata and a NULL geometry value.
python3 - "$TMP_DIR" <<'PYEOF'
import sys, json, struct
import pyarrow as pa

out = sys.argv[1]

# Minimal WKB for POINT (10 20): little-endian, type=1 (Point), x=10, y=20
def wkb_point(x, y):
    return struct.pack('<bIdd', 1, 1, x, y)

geo_meta = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {
        "geometry": {
            "encoding": "WKB",
            "geometry_types": ["Point"],
        }
    }
}

geometry = pa.array([wkb_point(10, 20), None], type=pa.binary())
name     = pa.array(["Point1", "Point2"], type=pa.utf8())
table    = pa.table({"geometry": geometry, "name": name})

meta = table.schema.metadata or {}
meta[b"geo"] = json.dumps(geo_meta).encode()
table = table.replace_schema_metadata(meta)

writer = pa.ipc.new_file(out + "/geo.arrow", table.schema)
writer.write_table(table)
writer.close()
PYEOF

echo "=== Arrow: schema inference with geoparquet parser disabled ==="
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(geometry) FROM file('$TMP_DIR/geo.arrow', Arrow) LIMIT 1 SETTINGS input_format_parquet_allow_geoparquet_parser=false"

echo "=== Arrow: NULL check with geoparquet parser disabled ==="
$CLICKHOUSE_LOCAL -q "SELECT geometry IS NULL as is_null, name FROM file('$TMP_DIR/geo.arrow', Arrow) ORDER BY name SETTINGS input_format_parquet_allow_geoparquet_parser=false"

echo "=== Arrow: schema inference with geoparquet parser enabled ==="
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(geometry) FROM file('$TMP_DIR/geo.arrow', Arrow) LIMIT 1 SETTINGS input_format_parquet_allow_geoparquet_parser=true"

echo "=== Arrow: NULL check with geoparquet parser enabled ==="
$CLICKHOUSE_LOCAL -q "SELECT geometry IS NULL as is_null, name FROM file('$TMP_DIR/geo.arrow', Arrow) ORDER BY name SETTINGS input_format_parquet_allow_geoparquet_parser=true"
