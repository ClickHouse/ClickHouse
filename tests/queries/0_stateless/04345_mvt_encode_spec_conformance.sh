#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Protobuf format requires the protobuf contrib, which is absent in the fast-test build.

# Decodes the bytes produced by MVTEncode back through the official Mapbox Vector Tile 2.1 wire schema
# (format_schemas/04345_mvt_vector_tile.proto) and asserts the decoded structure, rather than comparing
# opaque hex. The geometry command streams are checked against the worked examples in the specification
# (https://github.com/mapbox/vector-tile-spec/tree/master/2.1, section 4.3.5).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

TILE=$(mktemp "${CLICKHOUSE_TMP}/04345_tile.XXXXXX.bin")
trap 'rm -f "$TILE"' EXIT

SCHEMA="format_schema='${SCHEMADIR}/04345_mvt_vector_tile.proto:Tile'"

# Encode the bytes of a one-layer tile from the SQL expression in $1 into the temp file.
encode() {
    ${CLICKHOUSE_LOCAL} --query "SELECT ${1} INTO OUTFILE '${TILE}' TRUNCATE FORMAT RawBLOB"
}

# Decode the temp tile through the official schema. $1 = nested column structure, $2 = SELECT list,
# $3 = output format (default TSV).
decode() {
    ${CLICKHOUSE_LOCAL} --query "SELECT ${2} FROM file('${TILE}', 'ProtobufSingle', '${1}') SETTINGS ${SCHEMA} FORMAT ${3:-TSV}"
}

echo '-- Layer envelope decodes to version 2, the requested name and extent, and the property keys (MVT spec 4.1)'
encode "MVTEncode('points')((124.0, 3384.0)::Point::Geometry, tuple(toUInt64(3))::Tuple(cluster_count UInt64))"
decode 'layers Nested(version UInt32, name String, extent UInt32, keys Array(String))' \
    'layers.version, layers.name, layers.extent, layers.keys'

echo '-- Standard 4.3.5.1 Example Point (25,17): GeomType POINT and command stream [9,50,34]'
encode "MVTEncode('l')((25.0, 17.0)::Point::Geometry)"
decode 'layers Nested(features Nested(type UInt32, geometry Array(UInt32)))' \
    "layers.features.type[1] = [1], layers.features.geometry[1] = [[9, 50, 34]]"

echo '-- Standard 4.3.5.3 Example Linestring (2,2)(2,10)(10,10): GeomType LINESTRING and command stream [9,4,4,18,0,16,16,0]'
encode "MVTEncode('l')([(2.0, 2.0), (2.0, 10.0), (10.0, 10.0)]::LineString::Geometry)"
decode 'layers Nested(features Nested(type UInt32, geometry Array(UInt32)))' \
    "layers.features.type[1] = [2], layers.features.geometry[1] = [[9, 4, 4, 18, 0, 16, 16, 0]]"

echo '-- Standard 4.3.5.4 Example Polygon (3,6)(8,12)(20,34): GeomType POLYGON and command stream [9,6,12,18,10,12,24,44,15]'
encode "MVTEncode('l')([[(3.0, 6.0), (8.0, 12.0), (20.0, 34.0), (3.0, 6.0)]]::Polygon::Geometry)"
decode 'layers Nested(features Nested(type UInt32, geometry Array(UInt32)))' \
    "layers.features.type[1] = [3], layers.features.geometry[1] = [[9, 6, 12, 18, 10, 12, 24, 44, 15]]"

echo '-- Standard 4.3.5.5 Example Multi Linestring: a second MoveTo continues the same geometry [.. ,9,17,17,10,4,8]'
encode "MVTEncode('l')([[(2.0, 2.0), (2.0, 10.0), (10.0, 10.0)], [(1.0, 1.0), (3.0, 5.0)]]::MultiLineString::Geometry)"
decode 'layers Nested(features Nested(type UInt32, geometry Array(UInt32)))' \
    "layers.features.type[1] = [2], layers.features.geometry[1] = [[9, 4, 4, 18, 0, 16, 16, 0, 9, 17, 17, 10, 4, 8]]"

echo '-- Equivalence class: a polygon with a hole emits two rings, each terminated by ClosePath (command id 15)'
encode "MVTEncode('l')([[(0.0, 0.0), (0.0, 10.0), (10.0, 10.0), (10.0, 0.0), (0.0, 0.0)], [(3.0, 3.0), (3.0, 7.0), (7.0, 7.0), (7.0, 3.0), (3.0, 3.0)]]::Polygon::Geometry)"
decode 'layers Nested(features Nested(type UInt32, geometry Array(UInt32)))' \
    "layers.features.type[1] = [3], arrayCount(x -> x = 15, layers.features.geometry[1][1])"

echo '-- Equivalence class: each property type decodes to its MVT Value variant (spec 4.4): string/float/double/sint/uint/bool'
encode "MVTEncode('l')((0.0, 0.0)::Point::Geometry, tuple('s', toFloat32(1.5), toFloat64(2.5), toInt32(-7), toUInt32(8), true)::Tuple(str String, f32 Float32, f64 Float64, i Int32, u UInt32, b Bool))"
decode 'layers Nested(keys Array(String), values Nested(string_value String, float_value Float32, double_value Float64, sint_value Int64, uint_value UInt64, bool_value UInt8))' \
    'layers.keys[1] AS keys, layers.values.string_value[1] AS string_value, layers.values.float_value[1] AS float_value, layers.values.double_value[1] AS double_value, layers.values.sint_value[1] AS sint_value, layers.values.uint_value[1] AS uint_value, layers.values.bool_value[1] AS bool_value' \
    Vertical

echo '-- Equivalence class: Date decodes to uint_value and Date32 to sint_value (the days since epoch)'
encode "MVTEncode('l')((0.0, 0.0)::Point::Geometry, tuple(toDate('2020-01-01'), toDate32('2020-01-01'))::Tuple(d Date, d32 Date32))"
decode 'layers Nested(values Nested(uint_value UInt64, sint_value Int64))' \
    "layers.values.uint_value[1] = [18262, 0], layers.values.sint_value[1] = [0, 18262]"

echo '-- Equivalence class: feature_id_name emits the named element as Feature.id and excludes it from the tags'
encode "MVTEncode('l', 4096, 'fid')((0.0, 0.0)::Point::Geometry, tuple(toUInt64(42), 'x')::Tuple(fid UInt64, name String))"
decode 'layers Nested(keys Array(String), features Nested(id UInt64, tags Array(UInt32)), values Nested(string_value String))' \
    "layers.features.id[1] = [42], layers.keys[1] = ['name'], layers.features.tags[1] = [[0, 0]], layers.values.string_value[1] = ['x']"

echo '-- Equivalence class: a shared value is interned once (pool size 1); two distinct values give pool size 2'
encode "MVTEncode('l')(geom, tuple(v)::Tuple(c UInt64)) FROM (SELECT (10.0, 20.0)::Point::Geometry AS geom, toUInt64(5) AS v UNION ALL SELECT (30.0, 40.0)::Point::Geometry, toUInt64(5)) SETTINGS max_threads = 1"
decode 'layers Nested(values Nested(uint_value UInt64))' "length(layers.values.uint_value[1])"
encode "MVTEncode('l')(geom, tuple(v)::Tuple(c UInt64)) FROM (SELECT (10.0, 20.0)::Point::Geometry AS geom, toUInt64(5) AS v UNION ALL SELECT (30.0, 40.0)::Point::Geometry, toUInt64(6)) SETTINGS max_threads = 1"
decode 'layers Nested(values Nested(uint_value UInt64))' "length(layers.values.uint_value[1])"

echo '-- Equivalence class: stringify_unsupported encodes an Int128 as its exact decimal string_value'
encode "MVTEncode('l', 4096, '', 1)((0.0, 0.0)::Point::Geometry, tuple(toInt128('170141183460469231731687303715884105727'))::Tuple(big Int128))"
decode 'layers Nested(values Nested(string_value String))' \
    "layers.values.string_value[1]"

echo '-- Boundary: zoom 0 with extent 1 projects the world origin to pixel (0,0): command stream [9,0,0]'
encode "MVTEncode('l', 1)(MVTEncodeGeom((0.0, 0.0)::Point, 0, 0, 0, 1, 0, false))"
decode 'layers Nested(extent UInt32, features Nested(geometry Array(UInt32)))' \
    "layers.extent = [1], layers.features.geometry[1] = [[9, 0, 0]]"

echo '-- Boundary: at the maximum zoom (32) a point projects into the tile that contains it and encodes as a POINT feature'
# The tile that contains the point at zoom 32 (the projection diverges at the poles, so the tile is derived from the
# point rather than hard-coded). clip is on (default), so the projected coordinate stays within [0, extent].
encode "MVTEncode('l')(MVTEncodeGeom((13.37, 52.52)::Point, 32, 2306993961, 1408556690))"
decode 'layers Nested(features Nested(type UInt32))' \
    "layers.features.type[1] = [1]"

echo '-- Boundary: an extent at the upper end (1073741824) is recorded verbatim in the layer'
encode "MVTEncode('l', 1073741824)((1.0, 2.0)::Point::Geometry)"
decode 'layers Nested(extent UInt32)' "layers.extent = [1073741824]"
