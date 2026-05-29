-- Tests for the Mapbox Vector Tile encoding functions: mvtEncodeGeom (scalar projection) and mvtEncode (aggregate).

SELECT '-- mvtEncodeGeom: project a known point into tile-local pixel space';
SELECT mvtEncodeGeom(13.37, 52.52, 10, 550, 335);

SELECT '-- mvtEncodeGeom: out-of-range longitude/latitude are clamped to the projection limits';
SELECT mvtEncodeGeom(200, 50, 5, 10, 10) = mvtEncodeGeom(180, 50, 5, 10, 10);
SELECT mvtEncodeGeom(20, 100, 5, 10, 10) = mvtEncodeGeom(20, 85.05112877980659, 5, 10, 10);

SELECT '-- mvtEncodeGeom: a larger extent yields proportionally larger pixel coordinates';
SELECT mvtEncodeGeom(13.37, 52.52, 10, 550, 335, 8192);

SELECT '-- mvtEncodeGeom: out-of-range zoom and zero extent are errors';
SELECT mvtEncodeGeom(0, 0, 33, 0, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT mvtEncodeGeom(0, 0, 0, 0, 0, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The properties argument must be a named tuple; aliases inside tuple(...) are dropped, so names are set with a cast.

SELECT '-- mvtEncode: a single point feature with one uint attribute is a valid single-layer tile';
SELECT hex(mvtEncode('points')(tuple(124.0, 3384.0), tuple(toUInt64(3))::Tuple(cluster_count UInt64)));

SELECT '-- mvtEncode: geometry only, no properties';
SELECT hex(mvtEncode('points')(tuple(124.0, 3384.0)));

SELECT '-- mvtEncode: an empty group produces an empty tile';
SELECT length(mvtEncode('points')(tuple(0.0, 0.0))) FROM numbers(0);

DROP TABLE IF EXISTS mvt_points;
CREATE TABLE mvt_points (lon Float64, lat Float64, name String) ENGINE = Memory;
INSERT INTO mvt_points VALUES (13.37, 52.52, 'a') (13.37, 52.52, 'b') (13.40, 52.50, 'c');

SELECT '-- mvtEncode: clustering is expressed in SQL by grouping on the pixel geometry and counting';
SELECT geom, count() AS cluster_count
FROM (SELECT mvtEncodeGeom(lon, lat, 10, 550, 335) AS geom FROM mvt_points)
GROUP BY geom
ORDER BY geom;

SELECT '-- mvtEncode: a tile with several features sharing an attribute value interns it only once';
SELECT hex(mvtEncode('points')(geom, tuple(cluster_count)::Tuple(cluster_count UInt64)))
FROM
(
    SELECT tuple(10.0, 20.0) AS geom, toUInt64(5) AS cluster_count
    UNION ALL
    SELECT tuple(30.0, 40.0) AS geom, toUInt64(5) AS cluster_count
    ORDER BY geom
)
SETTINGS max_threads = 1;

SELECT '-- mvtEncode: each supported property type maps to its vector tile value type';
SELECT hex(mvtEncode('t')(
    tuple(0.0, 0.0),
    tuple('s', toFloat32(1.5), toFloat64(2.5), toInt32(-7), toUInt32(8), toDate('2020-01-01'))
        ::Tuple(str String, f32 Float32, f64 Float64, i Int32, u UInt32, d Date)));

SELECT '-- mvtEncode: a NULL property omits that attribute for the feature';
SELECT hex(mvtEncode('t')(
    tuple(0.0, 0.0),
    tuple(CAST(NULL, 'Nullable(String)'), toUInt32(8))::Tuple(str Nullable(String), u UInt32)));

SELECT '-- mvtEncode: LowCardinality property types are accepted';
SELECT hex(mvtEncode('t')(tuple(0.0, 0.0), tuple('x')::Tuple(str LowCardinality(String))));

SELECT '-- mvtEncode: the -State / -Merge combinators round-trip the aggregate state to the same tile';
SELECT hex(mvtEncodeMerge('points')(s))
FROM
(
    SELECT mvtEncodeState('points')(tuple(124.0, 3384.0), tuple(toUInt64(3))::Tuple(cluster_count UInt64)) AS s
);

SELECT '-- mvtEncode: a non-named properties tuple is rejected';
SELECT mvtEncode('t')(tuple(0.0, 0.0), tuple(1, 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- mvtEncode: an unsupported property type is rejected';
SELECT mvtEncode('t')(tuple(0.0, 0.0), tuple([1, 2])::Tuple(arr Array(UInt8))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- mvtEncode: the layer name parameter is required';
SELECT mvtEncode(tuple(0.0, 0.0)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE mvt_points;
