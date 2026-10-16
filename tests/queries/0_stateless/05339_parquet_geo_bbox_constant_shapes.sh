#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: the WebAssembly UDF engine and the Parquet format are not built in fasttest.
# Tag no-msan: the wasmtime engine behind WASM UDFs is not instrumented for MSan.
#
# Constant-shape coverage for `extractBboxFromFieldValue` (src/Common/GeoBbox.h) -- the helper
# that turns the *constant* argument of a spatial predicate into a bounding box.
# Documented behaviour: `input_format_parquet_spatial_filter_push_down`
# (docs/reference/settings/formats/input-format.mdx): "skip whole row groups ... based on
# spatial predicates in the WHERE clause and the geometry bounding box statistics".
#
# `pointInPolygon` cannot reach most shapes below: its `getReturnTypeImpl` rejects constants
# that are not rings/polygons, and for the ones it accepts `executeImpl` throws while the
# pipeline is still being built, i.e. before the Parquet reader initialises its row groups.
# A WASM UDF declared `is_spatial_predicate = 1` (same fixture as 04511) accepts any declared
# argument type and never throws, so every constant shape reaches the extractor.
#
# The fixture (from 04059) holds 4 rows in 2 row groups, all in Texas (x -99..-96, y 30..36).
# Every constant below has a bbox inside (0,0)-(3,3), i.e. disjoint from the data, and the UDF
# is unconditionally true, so the returned row count is the whole assertion:
#   well-formed constant   -> bbox extracted   -> both row groups pruned        -> 0 rows
#   malformed/unsupported  -> extraction fails -> pruning off, fail-closed      -> 4 rows

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="$CUR_DIR/data_parquet/04059_geo_spatial_pruning.parquet"
WASM_CONFIG="$CUR_DIR/../../config/config.d/wasm_udf.xml"

# Hand-built little-endian WKB, assembled from named pieces so it stays readable:
# `01` = little endian byte order, then a 4-byte geometry type, then counts and Float64 values.
F0=0000000000000000; F1=000000000000F03F   # 0.0 and 1.0 as little-endian IEEE-754
F2=0000000000000040; F3=0000000000000840   # 2.0 and 3.0
N1=01000000; N2=02000000; N5=05000000      # little-endian uint32 counts
T_PT=01000000; T_LS=02000000; T_PG=03000000; T_MPT=04000000; T_MLS=05000000; T_MPG=06000000
RING="$N1$N5$F0$F0$F2$F0$F2$F2$F0$F2$F0$F0"  # 1 ring, 5 points (0,0)(2,0)(2,2)(0,2)(0,0)
W_PT="01$T_PT$F1$F2"                         # Point(1,2)
W_LS="01$T_LS$N2$F0$F0$F1$F1"                # LineString (0,0)..(1,1)
W_PG="01$T_PG$RING"                          # Polygon, bbox (0,0)-(2,2)
W_MPT="01$T_MPT$N2""01$T_PT$F0$F0""01$T_PT$F1$F1"                    # MultiPoint (0,0),(1,1)
W_MLS="01$T_MLS$N2""01$T_LS$N2$F0$F0$F1$F1""01$T_LS$N2$F2$F2$F3$F3"  # MultiLineString
W_MPG="01$T_MPG$N1$W_PG"                                             # MultiPolygon, 1 polygon

# One UDF per declared constant type. All share the single content-agnostic `always_true`
# export, so the declared type of the second argument is the only thing that varies.
DEFS=""
for spec in "p2:Tuple(Float64, Float64)" "p1:Tuple(Float64)" \
            "ring:Array(Tuple(Float64, Float64))" "ring1:Array(Tuple(Float64))" \
            "poly:Array(Array(Tuple(Float64, Float64)))" "poly1:Array(Array(Tuple(Float64)))" \
            "multi:Array(Array(Array(Tuple(Float64, Float64))))" \
            "multi1:Array(Array(Array(Tuple(Float64))))" "num:UInt8" "wkb:String"
do
    DEFS+="CREATE FUNCTION geo_${spec%%:*} LANGUAGE WASM ABI BUFFERED_V1
    FROM 'geo_bbox_shapes' :: 'always_true'
    ARGUMENTS (geom Point, c ${spec#*:}) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;
"
done

$CLICKHOUSE_LOCAL --config-file "$WASM_CONFIG" --queries-file /dev/stdin <<EOF
INSERT INTO system.webassembly_modules (name, code)
SELECT 'geo_bbox_shapes', raw_blob FROM file('$CUR_DIR/wasm/spatial_predicate.wasm', RawBLOB);
$DEFS

-- Tuple constant: a point needs at least two numeric elements.
SELECT 'point_pair', count() FROM file('$FILE', Parquet) WHERE geo_p2(geometry, (1., 2.));
SELECT 'point_single', count() FROM file('$FILE', Parquet) WHERE geo_p1(geometry, tuple(1.));

-- Ring constant: validated as an assembled polygon (bg::correct + bg::is_valid), so a
-- self-intersecting "bow tie" ring is rejected even though every segment is well formed.
SELECT 'ring', count() FROM file('$FILE', Parquet)
WHERE geo_ring(geometry, [(0., 0.), (2., 0.), (2., 2.), (0., 2.), (0., 0.)]);
SELECT 'ring_short_tuples', count() FROM file('$FILE', Parquet) WHERE geo_ring1(geometry, [tuple(1.)]);
SELECT 'ring_self_intersecting', count() FROM file('$FILE', Parquet)
WHERE geo_ring(geometry, [(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)]);

-- Polygon constant: shell plus holes, validated after assembly, so a hole that lies
-- completely outside its shell is invalid even though both rings are individually simple.
SELECT 'polygon_with_hole', count() FROM file('$FILE', Parquet)
WHERE geo_poly(geometry, [[(0., 0.), (4., 0.), (4., 4.), (0., 4.), (0., 0.)],
                          [(1., 1.), (2., 1.), (2., 2.), (1., 2.), (1., 1.)]]);
SELECT 'polygon_short_tuples', count() FROM file('$FILE', Parquet) WHERE geo_poly1(geometry, [[tuple(1.)]]);
SELECT 'polygon_hole_outside_shell', count() FROM file('$FILE', Parquet)
WHERE geo_poly(geometry, [[(0., 0.), (2., 0.), (2., 2.), (0., 2.), (0., 0.)],
                          [(3., 3.), (4., 3.), (4., 4.), (3., 4.), (3., 3.)]]);
-- Not a polygon (one element is an empty array): falls back to element-wise recursion.
SELECT 'nested_array_recursion', count() FROM file('$FILE', Parquet)
WHERE geo_poly(geometry, [[(0., 0.), (2., 0.), (2., 2.), (0., 2.), (0., 0.)], []]);

-- Multipolygon constant.
SELECT 'multipolygon', count() FROM file('$FILE', Parquet)
WHERE geo_multi(geometry, [[[(0., 0.), (2., 0.), (2., 2.), (0., 2.), (0., 0.)]]]);
SELECT 'multipolygon_short_tuples', count() FROM file('$FILE', Parquet) WHERE geo_multi1(geometry, [[[tuple(1.)]]]);
SELECT 'multipolygon_self_intersecting', count() FROM file('$FILE', Parquet)
WHERE geo_multi(geometry, [[[(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)]]]);

-- A constant that is neither a tuple, an array nor a string carries no geometry at all.
SELECT 'plain_number', count() FROM file('$FILE', Parquet) WHERE geo_num(geometry, 42);

-- String constants are parsed as WKB, one query per geometry type the visitor handles.
SELECT 'wkb_point', count() FROM file('$FILE', Parquet) WHERE geo_wkb(geometry, unhex('$W_PT'));
SELECT 'wkb_linestring', count() FROM file('$FILE', Parquet) WHERE geo_wkb(geometry, unhex('$W_LS'));
SELECT 'wkb_polygon', count() FROM file('$FILE', Parquet) WHERE geo_wkb(geometry, unhex('$W_PG'));
SELECT 'wkb_multipoint', count() FROM file('$FILE', Parquet) WHERE geo_wkb(geometry, unhex('$W_MPT'));
SELECT 'wkb_multilinestring', count() FROM file('$FILE', Parquet) WHERE geo_wkb(geometry, unhex('$W_MLS'));
SELECT 'wkb_multipolygon', count() FROM file('$FILE', Parquet) WHERE geo_wkb(geometry, unhex('$W_MPG'));
SELECT 'wkb_unparseable', count() FROM file('$FILE', Parquet) WHERE geo_wkb(geometry, 'not-wkb');
EOF
