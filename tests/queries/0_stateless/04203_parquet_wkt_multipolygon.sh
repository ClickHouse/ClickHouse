#!/usr/bin/env bash
# Tags: no-fasttest
#
# Coverage gap: parseWKTMultiPolygon in src/Processors/Formats/Impl/ArrowGeoTypes.cpp
# is dispatched from parseWKTFormat at line 255-256 (`if (type == "MULTIPOLYGON")`) but
# was completely uncovered by the existing test suite — only Point/LineString/Polygon
# WKT-encoded GeoParquet files exist under tests/queries/0_stateless/data_parquet/
# (verified: 03445_geoparquet_wkt.parquet has only id, geometry, geometry_linestring,
# geometry_polygon columns). The PR fixed MULTILINESTRING dispatch (bug 1 was a typo
# that called parseWKTPolygon instead of parseWKTMultiLineString), but a similar
# regression in MULTIPOLYGON dispatch would currently go undetected.

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

# File 1: typed MultiPolygon column (exercises parseWKTMultiPolygon body
# at ArrowGeoTypes.cpp:214-225 plus the typed appendMultiPolygonToGeoColumn path).
write_geoparquet(
    out + "/mpoly_typed.parquet",
    ids=[1, 2],
    wkts=[
        "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 50 20, 50 50, 20 50, 20 20), (30 30, 50 50, 50 30, 30 30)))",
        "MULTIPOLYGON (((100 100, 200 100, 200 200, 100 200, 100 100)))",
    ],
    geometry_types=["MultiPolygon"],
)

# File 2: Mixed (Geometry/Variant) column containing Point + MULTIPOLYGON.
# Guards against a hypothetical regression in the MULTIPOLYGON dispatch that
# would mis-assign the value to the Polygon variant (analogous to the bug 1
# this PR fixes for MULTILINESTRING).
write_geoparquet(
    out + "/mpoly_mixed.parquet",
    ids=[1, 2],
    wkts=[
        "POINT (5 5)",
        "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 20)))",
    ],
    geometry_types=[],   # empty → Mixed / Geometry variant
)
PYEOF

GEO_SETTINGS="--input_format_parquet_use_native_reader_v3=1 --input_format_parquet_allow_geoparquet_parser=1"

# Test 1: typed MultiPolygon column — exercises parseWKTMultiPolygon (ArrowGeoTypes.cpp:214-225).
echo "=== MultiPolygon typed ==="
$CLICKHOUSE_LOCAL $GEO_SETTINGS -q \
    "SELECT id, geom FROM file('$TMP_DIR/mpoly_typed.parquet', Parquet) ORDER BY id"

# Test 2: Mixed (Geometry) column — variantType() must be 'MultiPolygon', not 'Polygon'.
# This guards the kMultiPolygonDiscriminator path at ArrowGeoTypes.cpp:378-379.
echo "=== MultiPolygon in Mixed column: variantType ==="
$CLICKHOUSE_LOCAL $GEO_SETTINGS -q \
    "SELECT id, variantType(geom) FROM file('$TMP_DIR/mpoly_mixed.parquet', Parquet) ORDER BY id"
