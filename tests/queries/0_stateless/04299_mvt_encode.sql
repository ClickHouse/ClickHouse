-- Tags: no-fasttest
-- ^ polygon clipping uses the wagyu contrib, which is disabled in the fast-test (ENABLE_LIBRARIES=0) build.
-- Tests for the Mapbox Vector Tile encoding functions: MVTEncodeGeom (scalar geometry projection) and
-- MVTEncode (aggregate tile assembly). The projection is Web Mercator; the tile format follows the
-- Mapbox Vector Tile specification 2.1.

SELECT '-- MVTEncodeGeom: project a known point into tile-local pixel space';
SELECT MVTEncodeGeom((13.37, 52.52)::Point, 10, 550, 335);

SELECT '-- MVTEncodeGeom: the return type is Geometry';
SELECT toTypeName(MVTEncodeGeom((13.37, 52.52)::Point, 10, 550, 335));

SELECT '-- MVTEncodeGeom: ST_AsMVTGeom is an alias';
SELECT ST_AsMVTGeom((13.37, 52.52)::Point, 10, 550, 335) = MVTEncodeGeom((13.37, 52.52)::Point, 10, 550, 335);

SELECT '-- MVTEncodeGeom: out-of-range longitude/latitude are clamped to the projection limits';
-- clip=false so the comparison tests the projection, not whether the (out-of-tile) point survives clipping.
SELECT MVTEncodeGeom((200.0, 50.0)::Point, 5, 10, 10, 4096, 256, false) = MVTEncodeGeom((180.0, 50.0)::Point, 5, 10, 10, 4096, 256, false);
SELECT MVTEncodeGeom((20.0, 100.0)::Point, 5, 10, 10, 4096, 256, false) = MVTEncodeGeom((20.0, 85.05112877980659)::Point, 5, 10, 10, 4096, 256, false);

SELECT '-- MVTEncodeGeom: a larger extent yields proportionally larger pixel coordinates';
SELECT MVTEncodeGeom((13.37, 52.52)::Point, 10, 550, 335, 8192);

SELECT '-- MVTEncodeGeom: (0,0) projects to pixel (0,0) at the centre tile for every zoom (no Web Mercator drift)';
SELECT
    z,
    MVTEncodeGeom((0.0, 0.0)::Point, z, t, t, 4096, 0, false) AS px_no_clip,
    MVTEncodeGeom((0.0, 0.0)::Point, z, t, t) AS px_clipped
FROM (SELECT CAST(arrayJoin([1, 16, 20, 24, 32]), 'UInt8') AS z, CAST(bitShiftLeft(toUInt64(1), z - 1), 'UInt32') AS t)
ORDER BY z;

SELECT '-- MVTEncodeGeom: a line is projected and returned as a MultiLineString';
SELECT MVTEncodeGeom([(13.4, 52.5), (13.5, 52.6), (13.6, 52.55)]::LineString, 10, 550, 335);

SELECT '-- MVTEncodeGeom: a polygon is projected and returned as a MultiPolygon';
SELECT MVTEncodeGeom([[(13.4, 52.5), (13.65, 52.5), (13.65, 52.65), (13.4, 52.65), (13.4, 52.5)]]::Polygon, 10, 550, 335);

SELECT '-- MVTEncodeGeom: out-of-range zoom and zero extent are errors';
SELECT MVTEncodeGeom((0.0, 0.0)::Point, 33, 0, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT MVTEncodeGeom((0.0, 0.0)::Point, 0, 0, 0, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The properties argument must be a named tuple; aliases inside tuple(...) are dropped, so names are set with a cast.

SELECT '-- MVTEncode: a single point feature with one uint attribute is a valid single-layer tile';
SELECT hex(MVTEncode('points')((124.0, 3384.0)::Point::Geometry, tuple(toUInt64(3))::Tuple(cluster_count UInt64)));

SELECT '-- MVTEncode: ST_AsMVT is an alias';
SELECT ST_AsMVT('points')((124.0, 3384.0)::Point::Geometry) = MVTEncode('points')((124.0, 3384.0)::Point::Geometry);

SELECT '-- MVTEncode: geometry only, no properties';
SELECT hex(MVTEncode('points')((124.0, 3384.0)::Point::Geometry));

-- A single feature per tile is used here: MVTEncode is order-dependent, so the exact bytes of a multi-feature tile
-- depend on the order rows reach the aggregate (not stable under parallelism), which would make the test flaky.
SELECT '-- MVTEncode: a line feature';
SELECT hex(MVTEncode('shapes')(MVTEncodeGeom([(13.4, 52.5), (13.5, 52.6)]::LineString, 10, 550, 335)));

SELECT '-- MVTEncode: a polygon feature';
SELECT hex(MVTEncode('shapes')(MVTEncodeGeom([[(13.4, 52.5), (13.65, 52.5), (13.65, 52.65), (13.4, 52.65), (13.4, 52.5)]]::Polygon, 10, 550, 335)));

SELECT '-- MVTEncode: a degenerate (collinear, zero-area) polygon ring is dropped, producing an empty tile';
SELECT length(MVTEncode('shapes')([[(100.0, 100.0), (200.0, 100.0), (300.0, 100.0), (100.0, 100.0)]]::Polygon::Geometry));

SELECT '-- MVTEncode: BFloat16 properties are encoded as float values (same as Float32)';
SELECT MVTEncode('t')((0.0, 0.0)::Point::Geometry, tuple(toBFloat16(1.5))::Tuple(c BFloat16))
     = MVTEncode('t')((0.0, 0.0)::Point::Geometry, tuple(toFloat32(1.5))::Tuple(c Float32));

SELECT '-- MVTEncode: a FixedString property trims its trailing NUL padding (matches the String value)';
SELECT MVTEncode('t')((0.0, 0.0)::Point::Geometry, tuple(toFixedString('ab', 4))::Tuple(c1 FixedString(4)))
     = MVTEncode('t')((0.0, 0.0)::Point::Geometry, tuple('ab')::Tuple(c1 String));

SELECT '-- MVTEncode: feature_id_name emits the named element as the Feature id and drops it from the tags';
-- One point feature: Feature.id = 42, geometry Point(0, 0), single tag name = 'x' (the id element 'fid' is not a key).
SELECT hex(MVTEncode('t', 4096, 'fid')((0.0, 0.0)::Point::Geometry, tuple(toUInt64(42), 'x')::Tuple(fid UInt64, name String)));

SELECT '-- MVTEncode: a NULL feature id is omitted, encoding identically to declaring no id';
-- Same feature with no Feature.id field: geometry Point(0, 0), single tag name = 'x'.
SELECT hex(MVTEncode('t', 4096, 'fid')((0.0, 0.0)::Point::Geometry, tuple(CAST(NULL, 'Nullable(UInt64)'), 'x')::Tuple(fid Nullable(UInt64), name String)));

SELECT '-- MVTEncode: feature_id_name must name an existing unsigned-integer element (signed integers are rejected)';
SELECT MVTEncode('t', 4096, 'missing')((0.0, 0.0)::Point::Geometry, tuple(toUInt64(1))::Tuple(fid UInt64)); -- { serverError BAD_ARGUMENTS }
SELECT MVTEncode('t', 4096, 'fid')((0.0, 0.0)::Point::Geometry, tuple('s')::Tuple(fid String)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT MVTEncode('t', 4096, 'fid')((0.0, 0.0)::Point::Geometry, tuple(toInt64(1))::Tuple(fid Int64)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- MVTEncode: stringify_unsupported encodes otherwise-unsupported types (e.g. Int128) as their decimal text value';
-- One point feature: single tag big = the Int128 value as a string_value.
SELECT hex(MVTEncode('t', 4096, '', 1)((0.0, 0.0)::Point::Geometry, tuple(toInt128('170141183460469231731687303715884105727'))::Tuple(big Int128)));
SELECT MVTEncode('t')((0.0, 0.0)::Point::Geometry, tuple(toInt128(1))::Tuple(big Int128)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- MVTEncode: a sub-pixel line whose vertices round to one point is dropped (no zero-delta command)';
SELECT length(MVTEncode('shapes')(MVTEncodeGeom([(13.37000, 52.52000), (13.37001, 52.52000)]::LineString, 10, 550, 335)));

SELECT '-- MVTEncode: adjacent duplicate vertices are pruned, but a line with two distinct vertices still encodes';
SELECT length(MVTEncode('shapes')([(100.0, 100.0), (100.0, 100.0), (200.0, 200.0)]::LineString::Geometry)) > 0;

SELECT '-- MVTEncode: a tiny polygon near Int32::max keeps its area (exact integer shoelace, no double cancellation)';
SELECT length(MVTEncode('shapes')([[(2147483640.0, 2147483640.0), (2147483641.0, 2147483640.0), (2147483641.0, 2147483641.0), (2147483640.0, 2147483641.0), (2147483640.0, 2147483640.0)]]::Polygon::Geometry)) > 0;

SELECT '-- MVTEncode: an empty group produces an empty tile';
SELECT length(MVTEncode('points')((0.0, 0.0)::Point::Geometry)) FROM numbers(0);

SELECT '-- MVTEncode: NULL (clipped-out) geometry rows are skipped, so they do not change the tile';
SELECT
(
    SELECT length(MVTEncode('points')(geom))
    FROM
    (
        SELECT MVTEncodeGeom((13.5, 52.6)::Point, 10, 550, 335) AS geom
        UNION ALL SELECT MVTEncodeGeom((0.0, 0.0)::Point, 10, 550, 335)
    )
) = length(MVTEncode('points')(MVTEncodeGeom((13.5, 52.6)::Point, 10, 550, 335)));

DROP TABLE IF EXISTS mvt_points;
CREATE TABLE mvt_points (lon Float64, lat Float64, name String) ENGINE = Memory;
INSERT INTO mvt_points VALUES (13.37, 52.52, 'a') (13.37, 52.52, 'b') (13.40, 52.50, 'c');

SELECT '-- MVTEncode: clustering is expressed in SQL by grouping on the pixel geometry and counting';
-- Grouping by a Geometry (a Variant) requires opting in, because grouping by Variant is restricted by default.
SELECT geom, count() AS cluster_count
FROM (SELECT MVTEncodeGeom((lon, lat)::Point, 10, 550, 335) AS geom FROM mvt_points)
GROUP BY geom
ORDER BY geom
SETTINGS allow_suspicious_types_in_group_by = 1, allow_suspicious_types_in_order_by = 1;

SELECT '-- MVTEncode: a shared attribute value is interned once, so a tile with one distinct value is smaller';
-- The tile is compared by length rather than by exact bytes because MVTEncode is order-dependent: the order
-- of the feature messages (and hence the exact bytes) depends on the order rows reach the aggregate, but the
-- total length does not. A shared value yields a one-entry value pool, so the tile is shorter than when the
-- two features carry distinct values.
SELECT
    length(MVTEncode('points')(geom, tuple(toUInt64(5))::Tuple(cluster_count UInt64)))
        < length(MVTEncode('points')(geom, tuple(cluster_count)::Tuple(cluster_count UInt64)))
FROM
(
    SELECT (10.0, 20.0)::Point::Geometry AS geom, toUInt64(5) AS cluster_count
    UNION ALL
    SELECT (30.0, 40.0)::Point::Geometry AS geom, toUInt64(6) AS cluster_count
);

SELECT '-- MVTEncode: each supported property type maps to its vector tile value type';
SELECT hex(MVTEncode('t')(
    (0.0, 0.0)::Point::Geometry,
    tuple('s', toFloat32(1.5), toFloat64(2.5), toInt32(-7), toUInt32(8), toDate('2020-01-01'), toDate32('2020-01-01'), true)
        ::Tuple(str String, f32 Float32, f64 Float64, i Int32, u UInt32, d Date, d32 Date32, b Bool)));

SELECT '-- MVTEncode: a NULL property omits that attribute for the feature';
SELECT hex(MVTEncode('t')(
    (0.0, 0.0)::Point::Geometry,
    tuple(CAST(NULL, 'Nullable(String)'), toUInt32(8))::Tuple(str Nullable(String), u UInt32)));

SELECT '-- MVTEncode: LowCardinality property types are accepted';
SELECT hex(MVTEncode('t')((0.0, 0.0)::Point::Geometry, tuple('x')::Tuple(str LowCardinality(String))));

SELECT '-- MVTEncode: the -State / -Merge combinators round-trip the aggregate state to the same tile';
SELECT hex(MVTEncodeMerge('points')(s))
FROM
(
    SELECT MVTEncodeState('points')((124.0, 3384.0)::Point::Geometry, tuple(toUInt64(3))::Tuple(cluster_count UInt64)) AS s
);

SELECT '-- MVTEncode: the geometry argument must be of type Geometry';
SELECT MVTEncode('t')(tuple(0.0, 0.0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- MVTEncode: a non-named properties tuple is rejected';
SELECT MVTEncode('t')((0.0, 0.0)::Point::Geometry, tuple(1, 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- MVTEncode: an unsupported property type is rejected';
SELECT MVTEncode('t')((0.0, 0.0)::Point::Geometry, tuple([1, 2])::Tuple(arr Array(UInt8))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- MVTEncode: the layer name parameter is required';
SELECT MVTEncode((0.0, 0.0)::Point::Geometry); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE mvt_points;
