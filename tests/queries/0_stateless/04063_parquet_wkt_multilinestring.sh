#!/usr/bin/env bash
# Tags: no-fasttest
#
# Regression test for two bugs in parseWKTFormat (ArrowGeoTypes.cpp):
#
# Bug 1: MULTILINESTRING called parseWKTPolygon instead of a proper parser.
#   - For a typed MultiLineString GeoParquet column this threw BAD_ARGUMENTS.
#   - For a Mixed (Geometry) column it silently stored the geometry under the
#     Polygon variant discriminator instead of MultiLineString (data corruption
#     that is visible via variantType()).
#
# Bug 2: type.back() was called on a potentially empty std::string (undefined
#   behaviour when input starts with '('). After the fix an empty type produces
#   a clear BAD_ARGUMENTS exception.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$TMP_DIR" <<'PYEOF'
import sys, json
import pyarrow as pa
import pyarrow.parquet as pq

out = sys.argv[1]

def write_geoparquet(path, ids, wkts, geometry_types):
    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geom",
        "columns": {
            "geom": {
                "encoding": "WKT",
                "geometry_types": geometry_types,
            }
        }
    }
    table = pa.table({
        "id":   pa.array(ids, type=pa.int32()),
        "geom": pa.array(wkts, type=pa.utf8()),
    })
    meta = table.schema.metadata or {}
    meta[b"geo"] = json.dumps(geo_meta).encode()
    table = table.replace_schema_metadata(meta)
    pq.write_table(table, path)

# File 1: explicit MultiLineString type (was throwing BAD_ARGUMENTS before fix)
write_geoparquet(
    out + "/mls_typed.parquet",
    ids=[1, 2],
    wkts=[
        "MULTILINESTRING ((0 0, 1 1, 2 0), (3 3, 4 4, 5 3))",
        "MULTILINESTRING ((10 10, 11 11), (20 20, 21 21, 22 20))",
    ],
    geometry_types=["MultiLineString"],
)

# File 2: Mixed geometry column (geometry_types=[]) containing Point and MULTILINESTRING.
# Before fix: MULTILINESTRING entries were stored under the Polygon variant discriminator.
write_geoparquet(
    out + "/mls_mixed.parquet",
    ids=[1, 2, 3],
    wkts=[
        "POINT (5 5)",
        "MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))",
        "MULTILINESTRING ((7 7, 8 8, 9 7))",
    ],
    geometry_types=[],   # empty → Mixed / Geometry variant
)

# File 3: malformed WKT with no type keyword — triggers the empty-string UB (bug 2)
write_geoparquet(
    out + "/wkt_empty_type.parquet",
    ids=[1],
    wkts=["((0 0, 1 1))"],
    geometry_types=["LineString"],
)
PYEOF

GEO_SETTINGS="--input_format_parquet_use_native_reader_v3=1 --input_format_parquet_allow_geoparquet_parser=1"

# Test 1: typed MultiLineString column must return geometry data, not throw.
echo "=== MultiLineString typed ==="
$CLICKHOUSE_LOCAL $GEO_SETTINGS -q \
    "SELECT id, geom FROM file('$TMP_DIR/mls_typed.parquet', Parquet) ORDER BY id"

# Test 2: Mixed (Geometry) column — variantType() must be 'MultiLineString', not 'Polygon'.
echo "=== MultiLineString in Mixed column: variantType ==="
$CLICKHOUSE_LOCAL $GEO_SETTINGS -q \
    "SELECT id, variantType(geom) FROM file('$TMP_DIR/mls_mixed.parquet', Parquet) ORDER BY id"

# Test 3: malformed WKT (empty type string) must produce BAD_ARGUMENTS, not crash/UB.
echo "=== malformed WKT (empty type) ==="
$CLICKHOUSE_LOCAL $GEO_SETTINGS -q \
    "SELECT geom FROM file('$TMP_DIR/wkt_empty_type.parquet', Parquet)" 2>&1 \
    | grep -c "BAD_ARGUMENTS"
