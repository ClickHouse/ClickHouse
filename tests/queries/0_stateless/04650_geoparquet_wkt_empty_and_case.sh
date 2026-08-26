#!/usr/bin/env bash
# Tags: no-fasttest
#       ^ no Parquet support in fasttest
#
# parseWKTFormat (ArrowGeoTypes.cpp) decodes WKT geometry columns of GeoParquet /
# Arrow files. It rejected two families of WKT that the SQL function readWKT accepts:
#   A. a non-uppercase type keyword ('point(1 2)'),
#   B. an empty container, in both spellings: '<TYPE> EMPTY' and '<TYPE>()'.
# Class B also broke round-tripping of ClickHouse's own wkt() output, which emits
# 'LINESTRING()' / 'POLYGON()' for empty geometries.
#
# A GeoParquet file cannot be produced from SQL (ClickHouse only writes WKB geo
# columns), hence the shell test with a pyarrow generator.
#
# POINT is deliberately asymmetric: a ClickHouse Point is Tuple(Float64, Float64)
# with no empty representation, so 'POINT EMPTY' / 'POINT()' stay rejected (#110692).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$TMP_DIR" <<'PYEOF'
import sys, json, os
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.ipc as ipc

out = sys.argv[1]

def geo_meta(geometry_types, encoding="WKT"):
    return {
        "version": "1.1.0",
        "primary_column": "geom",
        "columns": {"geom": {"encoding": encoding, "geometry_types": geometry_types}},
    }

def make_table(wkts, geometry_types):
    table = pa.table({
        "id":   pa.array(list(range(1, len(wkts) + 1)), type=pa.int32()),
        "geom": pa.array(wkts, type=pa.utf8()),
    })
    meta = table.schema.metadata or {}
    meta[b"geo"] = json.dumps(geo_meta(geometry_types)).encode()
    return table.replace_schema_metadata(meta)

def write_geoparquet(name, wkts, geometry_types=[]):
    pq.write_table(make_table(wkts, geometry_types), os.path.join(out, name + ".parquet"))

def write_geoarrow(name, wkts, geometry_types=[]):
    table = make_table(wkts, geometry_types)
    with ipc.new_file(os.path.join(out, name + ".arrow"), table.schema) as writer:
        writer.write_table(table)

# 1. Case-insensitive type keyword, all six types plus a mixed-case spelling.
write_geoparquet("case", [
    "point(1 2)",
    "LineString(1 1, 2 2)",
    "polygon((0 0,1 0,1 1,0 0))",
    "multipoint(1 1, 2 2)",
    "multilinestring((1 1,2 2))",
    "multipolygon(((0 0,1 0,1 1,0 0)))",
    "POiNt(1 2)",
])

# 2. The tagged EMPTY spelling, all five container types, any case, any separator.
write_geoparquet("tagged_empty", [
    "LINESTRING EMPTY",
    "POLYGON EMPTY",
    "MULTIPOINT EMPTY",
    "MULTILINESTRING EMPTY",
    "MULTIPOLYGON EMPTY",
    "linestring empty",
    "LineString Empty",
    "LINESTRING  EMPTY",
    "LINESTRING\tEMPTY",
    "LINESTRING\nEMPTY",
    "  LINESTRING EMPTY  ",
])

# 3. The empty-parenthesis spelling, including a nested '()' at every depth and
#    mixed non-empty/empty element lists.
write_geoparquet("empty_list", [
    "LINESTRING()",
    "POLYGON()",
    "MULTIPOINT()",
    "MULTILINESTRING()",
    "MULTIPOLYGON()",
    "LINESTRING ( )",
    "POLYGON( ( ) )",
    "POLYGON(())",
    "POLYGON((),())",
    "MULTILINESTRING(())",
    "MULTIPOLYGON((()))",
    "MULTIPOLYGON(())",
    "MULTILINESTRING((1 1,2 2),())",
    "POLYGON((0 0,1 0,1 1,0 0),())",
])

# 4. Non-empty values, to pin that the new branches did not change them.
write_geoparquet("control", [
    "POINT(1 2)",
    "LINESTRING(1 1, 2 2)",
    "POLYGON((0 0,1 0,1 1,0 0))",
    "MULTIPOINT((1 1),(2 2))",
])

# 5. Concrete (non-Mixed) geo columns take a different append path than the
#    Geometry Variant, so cover both.
write_geoparquet("typed_ls",    ["LINESTRING EMPTY", "LINESTRING()"],   ["LineString"])
write_geoparquet("typed_poly",  ["POLYGON EMPTY", "POLYGON(())"],       ["Polygon"])
write_geoparquet("typed_mpoly", ["MULTIPOLYGON EMPTY", "MULTIPOLYGON(())"], ["MultiPolygon"])
write_geoparquet("typed_point", ["point(1 2)"],                         ["Point"])

# 6. Both Arrow readers (native IPC and the legacy Arrow reader) use the same
#    decoder as Parquet.
write_geoarrow("arrow", ["point(1 2)", "LINESTRING EMPTY", "POLYGON(())", "MULTIPOLYGON((()))"])

# 7. Rejected values, one file each so the throwing row is isolated.
#    First group: readWKT rejects these too.
#    Second group: the importer stays stricter than readWKT, which fabricates
#    coordinates for them ('POINT()' -> (0,0), 'POINT(1)' -> (1,0)) or accepts a
#    dimension tag the importer does not support.
bad_values = [
    "LINESTRINGEMPTY",
    "LINESTRING EMPTYX",
    "LINESTRING E(1 1, 2 2)",
    "LINESTRING EMPTY trailing",
    "POLYGON(EMPTY)",
    "POLYGON(empty)",
    "MULTILINESTRING(EMPTY)",
    "MULTIPOLYGON(EMPTY)",
    "MULTIPOLYGON(EMPTY,EMPTY)",
    # EMPTY is only valid after the type keyword, never at element position. A separator
    # before ')' is what actually pins this: without it, 'EMPTY)' is one token and would be
    # rejected even by an element-position EMPTY branch.
    "POLYGON(EMPTY )",
    "MULTILINESTRING(EMPTY )",
    "MULTIPOLYGON(EMPTY )",
    "MULTIPOLYGON((EMPTY ))",
    "MULTIPOINT(1 1,())",
    "POINT Z (1 2 3)",
    "LINESTRING Z EMPTY",
    "LINESTRING ZM EMPTY",
    "POINT EMPTY",
    "point empty",
    "POINT Z EMPTY",
    "POINT M EMPTY",
    "POINT ZM EMPTY",
    "POINT()",
    "MULTIPOINT(())",
    "POINT(1)",
    "LINESTRING(1 1, 2)",
    "LINESTRING M EMPTY",
    "LINESTRING M (1 1,2 2)",
]
for i, wkt in enumerate(bad_values):
    write_geoparquet(f"bad_{i}", [wkt])

# All rejections run in ONE clickhouse-local process, driven by this generated file.
# One process per case is what a reader would expect, but it is far too slow: each
# rejection formats a symbolized stack trace, and on a sanitizer build 28 separate
# processes measured ~40s against ~3s batched, which alone would push the test past
# the 180s flaky-check limit. --ignore-error keeps the batch going after a throw.
# The oracle is the PRESENCE of an imported row after each marker, not an error
# string, so 'ctl' (a valid value, run before and after) is required: without it
# the whole section would pass vacuously if no statement produced any output.
write_geoparquet("ctl", ["POINT(7 8)"])
with open(os.path.join(out, "rejected.sql"), "w") as sql:
    def case(name):
        path = os.path.join(out, name + ".parquet")
        sql.write("SELECT '%s';\n" % name)
        sql.write("SELECT '%s IMPORTED', toString(geom) FROM file('%s', Parquet);\n" % (name, path))
    case("ctl")
    for i in range(len(bad_values)):
        case("bad_%d" % i)
    case("ctl")
PYEOF

PARQUET_SETTINGS="--input_format_parquet_allow_geoparquet_parser=1"

for name in case tagged_empty empty_list control; do
    echo "-- $name"
    $CLICKHOUSE_LOCAL $PARQUET_SETTINGS -q \
        "SELECT id, variantType(geom), geom FROM file('$TMP_DIR/$name.parquet', Parquet) ORDER BY id"
done

for name in typed_ls typed_poly typed_mpoly typed_point; do
    echo "-- $name"
    $CLICKHOUSE_LOCAL $PARQUET_SETTINGS -q \
        "SELECT id, toTypeName(geom), geom FROM file('$TMP_DIR/$name.parquet', Parquet) ORDER BY id"
done

# Both Arrow readers decode WKT through the same function.
for native in 1 0; do
    echo "-- arrow native=$native"
    $CLICKHOUSE_LOCAL --input_format_arrow_use_native_reader=$native -q \
        "SELECT id, variantType(geom), geom FROM file('$TMP_DIR/arrow.arrow', Arrow) ORDER BY id"
done

# wkt() emits the empty-parenthesis spelling, so its output must import back.
echo "-- round trip"
$CLICKHOUSE_LOCAL -q "SELECT wkt(CAST([], 'LineString')), wkt(CAST([[]], 'Polygon'))"
python3 - "$TMP_DIR" <<'PYEOF'
import sys, json, os
import pyarrow as pa
import pyarrow.parquet as pq
out = sys.argv[1]
table = pa.table({
    "id":   pa.array([1, 2], type=pa.int32()),
    "geom": pa.array(["LINESTRING()", "POLYGON()"], type=pa.utf8()),
})
meta = table.schema.metadata or {}
meta[b"geo"] = json.dumps({
    "version": "1.1.0", "primary_column": "geom",
    "columns": {"geom": {"encoding": "WKT", "geometry_types": []}},
}).encode()
pq.write_table(table.replace_schema_metadata(meta), os.path.join(out, "roundtrip.parquet"))
PYEOF
$CLICKHOUSE_LOCAL $PARQUET_SETTINGS -q \
    "SELECT id, variantType(geom), geom FROM file('$TMP_DIR/roundtrip.parquet', Parquet) ORDER BY id"

# Invalid values must stay rejected. A 'bad_N' line with no 'bad_N IMPORTED' line
# after it means that value produced no row, i.e. it was rejected; the two 'ctl'
# cases must each be followed by their IMPORTED line, proving the batch does
# detect an accepted value.
echo "-- rejected"
$CLICKHOUSE_LOCAL $PARQUET_SETTINGS --ignore-error \
    --queries-file "$TMP_DIR/rejected.sql"
