#!/usr/bin/env bash
# Tags: no-fasttest
#       ^ no Parquet support in fasttest
#
# Regression test: parseWKTFormat (ArrowGeoTypes.cpp) must match the SQL readWKT
# dispatcher and the WKT grammar (boost::geometry::read_wkt tokenizes on " \n\t\r").
# Three parts:
#   1. Leading whitespace before the type keyword ('  POINT (1 2)') used to give
#      BAD_ARGUMENTS (type "  POINT" matched nothing). Sibling of issue #110700.
#   2. Interior whitespace between the type keyword and '(', and between/around
#      coordinates ('POINT\t(1 2)', 'POINT(1  2)', 'LINESTRING(1 1,\n2 2)') used
#      to throw BAD_ARGUMENTS / CANNOT_PARSE_NUMBER, or silently drop a coordinate,
#      while readWKT parsed them. Both are now consistent.
#   3. Trailing / interior non-separator garbage ('POINT x(1 2)', 'POINT(1 2) x',
#      'POINT(1 2))', 'LINESTRING(1 1 xx, 2 2)', missing ')') used to be SILENTLY
#      accepted here (wrong data on import) while readWKT rejects them. Now the
#      import rejects them too (BAD_ARGUMENTS).

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
        "columns": {"geom": {"encoding": "WKT", "geometry_types": geometry_types}},
    }
    table = pa.table({
        "id":   pa.array(ids, type=pa.int32()),
        "geom": pa.array(wkts, type=pa.utf8()),
    })
    meta = table.schema.metadata or {}
    meta[b"geo"] = json.dumps(geo_meta).encode()
    table = table.replace_schema_metadata(meta)
    pq.write_table(table, path)

# Mixed (Geometry) column with leading whitespace (spaces / tab) before the type.
write_geoparquet(
    out + "/wkt_ws.parquet",
    ids=[1, 2, 3, 4, 5],
    wkts=[
        "  POINT (1 2)",
        "\tLINESTRING (0 0, 1 1)",
        "  POLYGON ((1 0, 10 0, 10 10, 0 10, 1 0))",
        " MULTILINESTRING ((1 1, 2 2), (3 3, 4 4))",
        "  MULTIPOLYGON (((2 0, 10 0, 10 10, 0 10, 2 0)))",
    ],
    geometry_types=[],
)

# Interior whitespace: tab / newline / crlf between the type keyword and '(',
# multiple spaces or tab between coordinates, newline after a coordinate comma.
write_geoparquet(
    out + "/wkt_interior.parquet",
    ids=[1, 2, 3, 4, 5],
    wkts=[
        "POINT\t(1 2)",
        "POINT\r\n(1 2)",
        "POINT(1  2)",
        "POINT(1\t2)",
        "LINESTRING(1 1,\n2 2)",
    ],
    geometry_types=[],
)

# Malformed values readWKT rejects: non-separator garbage before '(', trailing
# garbage after the geometry, an extra ')', garbage inside the coordinate list,
# and a missing ')'. Each in its own file so the throwing row is isolated.
bad_values = [
    "POINT x(1 2)",
    "POINT(1 2) trailing",
    "POINT(1 2))",
    "LINESTRING(1 1, 2 2) zzz",
    "LINESTRING(1 1 xx, 2 2)",
    "POINT(1 2",
]
for i, wkt in enumerate(bad_values):
    write_geoparquet(out + f"/wkt_bad_{i}.parquet", ids=[1], wkts=[wkt], geometry_types=[])
PYEOF

GEO_SETTINGS="--input_format_parquet_use_native_reader_v3=1 --input_format_parquet_allow_geoparquet_parser=1"

$CLICKHOUSE_LOCAL $GEO_SETTINGS -q \
    "SELECT id, variantType(geom), geom FROM file('$TMP_DIR/wkt_ws.parquet', Parquet) ORDER BY id"

$CLICKHOUSE_LOCAL $GEO_SETTINGS -q \
    "SELECT id, variantType(geom), geom FROM file('$TMP_DIR/wkt_interior.parquet', Parquet) ORDER BY id"

# Malformed inputs must be rejected (consistent with readWKT), not silently accepted.
for i in 0 1 2 3 4 5; do
    $CLICKHOUSE_LOCAL $GEO_SETTINGS -q \
        "SELECT geom FROM file('$TMP_DIR/wkt_bad_$i.parquet', Parquet)" 2>&1 \
        | grep -c -F "BAD_ARGUMENTS"
done
