-- `Boost.Geometry` must not turn an overflowing intersection calculation into an empty result.
-- The rectangles have an analytical answer at every scale, independent of Boost overlay.
SET max_threads = 1;

CREATE TABLE extreme_geo_input (exponent Int16, shift Int8, id UInt8, p Polygon) ENGINE = Memory;
CREATE TABLE extreme_geo_result (kind String, exponent Int16, shift Int8, result MultiPolygon) ENGINE = Memory;

INSERT INTO extreme_geo_input
WITH pow(10., exponent) AS scale, (number + toFloat64(shift)) * scale AS lower, lower + 2 * scale AS upper
SELECT exponent, shift, number,
    CAST([[(lower, lower), (lower, upper), (upper, upper), (upper, lower), (lower, lower)]], 'Polygon')
FROM numbers(2) AS rows
CROSS JOIN (SELECT arrayJoin([0, 20, 50, 100, 103, 105, 120, 154, 200, 300, 307]) AS exponent) AS scales
CROSS JOIN (SELECT arrayJoin([-4, 0, 4]) AS shift) AS shifts;

INSERT INTO extreme_geo_result
SELECT 'intersection_direct', exponent, shift, groupPolygonIntersection(p)
FROM extreme_geo_input GROUP BY exponent, shift;

INSERT INTO extreme_geo_result
SELECT 'intersection_reverse', exponent, shift, groupPolygonIntersection(p)
FROM (SELECT * FROM extreme_geo_input ORDER BY exponent, shift, id DESC) GROUP BY exponent, shift;

INSERT INTO extreme_geo_result
SELECT 'intersection_merge', exponent, shift, groupPolygonIntersectionMerge(s)
FROM (SELECT exponent, shift, groupPolygonIntersectionState(p) AS s FROM extreme_geo_input GROUP BY exponent, shift, id)
GROUP BY exponent, shift;

INSERT INTO extreme_geo_result
SELECT 'intersection_serialized_result', exponent, shift,
    groupPolygonIntersectionMerge(CAST(unhex(hex(s)), 'AggregateFunction(groupPolygonIntersection, Polygon)'))
FROM (SELECT exponent, shift, groupPolygonIntersectionState(p) AS s FROM extreme_geo_input GROUP BY exponent, shift)
GROUP BY exponent, shift;

INSERT INTO extreme_geo_result
SELECT 'intersection_serialized_merge', exponent, shift,
    groupPolygonIntersectionMerge(CAST(unhex(hex(s)), 'AggregateFunction(groupPolygonIntersection, Polygon)'))
FROM (SELECT exponent, shift, groupPolygonIntersectionState(p) AS s FROM extreme_geo_input GROUP BY exponent, shift, id)
GROUP BY exponent, shift;

INSERT INTO extreme_geo_result
SELECT 'union_direct', exponent, shift, groupPolygonUnion(p)
FROM extreme_geo_input GROUP BY exponent, shift;

INSERT INTO extreme_geo_result
SELECT 'union_reverse', exponent, shift, groupPolygonUnion(p)
FROM (SELECT * FROM extreme_geo_input ORDER BY exponent, shift, id DESC) GROUP BY exponent, shift;

INSERT INTO extreme_geo_result
SELECT 'union_merge', exponent, shift, groupPolygonUnionMerge(s)
FROM (SELECT exponent, shift, groupPolygonUnionState(p) AS s FROM extreme_geo_input GROUP BY exponent, shift, id)
GROUP BY exponent, shift;

INSERT INTO extreme_geo_result
SELECT 'union_serialized_result', exponent, shift,
    groupPolygonUnionMerge(CAST(unhex(hex(s)), 'AggregateFunction(groupPolygonUnion, Polygon)'))
FROM (SELECT exponent, shift, groupPolygonUnionState(p) AS s FROM extreme_geo_input GROUP BY exponent, shift)
GROUP BY exponent, shift;

INSERT INTO extreme_geo_result
SELECT 'union_serialized_merge', exponent, shift,
    groupPolygonUnionMerge(CAST(unhex(hex(s)), 'AggregateFunction(groupPolygonUnion, Polygon)'))
FROM (SELECT exponent, shift, groupPolygonUnionState(p) AS s FROM extreme_geo_input GROUP BY exponent, shift, id)
GROUP BY exponent, shift;

-- Divide by the input scale before checking area: the unscaled area itself can exceed `Float64`.
-- Check the entire vertex set as well as area, closure, and component/ring counts.
WITH
    pow(10., exponent) AS scale,
    arrayMap(polygon -> arrayMap(ring -> arrayMap(point ->
        (round(point.1 / scale - shift, 8), round(point.2 / scale - shift, 8)), ring), polygon), result) AS normalized,
    arraySort(arrayDistinct(arrayFlatten(normalized))) AS vertices,
    startsWith(kind, 'intersection') AS is_intersection,
    if(is_intersection,
        [(1., 1.), (1., 2.), (2., 1.), (2., 2.)],
        [(0., 0.), (0., 2.), (1., 2.), (1., 3.), (2., 0.), (2., 1.), (3., 1.), (3., 3.)]) AS expected_vertices
SELECT kind, count(), countIf(
    length(normalized) = 1 AND length(normalized[1]) = 1
    AND length(normalized[1][1]) = if(is_intersection, 5, 9)
    AND normalized[1][1][1] = normalized[1][1][-1]
    AND vertices = expected_vertices
    AND polygonAreaCartesian(CAST(normalized, 'MultiPolygon')) = if(is_intersection, 1, 7))
FROM extreme_geo_result GROUP BY kind ORDER BY kind;

-- Each coordinate is finite, but an edge difference exceeds the `Float64` range.
-- The input rings are open and have the opposite orientation to the aggregate representation.
TRUNCATE TABLE extreme_geo_input;
INSERT INTO extreme_geo_input VALUES
    (308, 0, 0, [[(-1e308, -1e308), (1e308, -1e308), (1e308, 1e308), (-1e308, 1e308)]]),
    (308, 0, 1, [[(-5e307, -5e307), (1.5e308, -5e307), (1.5e308, 1.5e308), (-5e307, 1.5e308)]]);

SELECT 'intersection_overflowing_difference', length(result),
    round(polygonAreaCartesian(CAST(arrayMap(polygon -> arrayMap(ring ->
        arrayMap(point -> (point.1 / 1e308, point.2 / 1e308), ring), polygon), result), 'MultiPolygon')), 8)
FROM (SELECT groupPolygonIntersection(p) AS result FROM extreme_geo_input);
SELECT 'union_overflowing_difference', length(result),
    round(polygonAreaCartesian(CAST(arrayMap(polygon -> arrayMap(ring ->
        arrayMap(point -> (point.1 / 1e308, point.2 / 1e308), ring), polygon), result), 'MultiPolygon')), 8)
FROM (SELECT groupPolygonUnion(p) AS result FROM extreme_geo_input);

DROP TABLE extreme_geo_result;
DROP TABLE extreme_geo_input;
