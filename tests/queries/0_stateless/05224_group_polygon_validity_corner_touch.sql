SET max_threads = 1;

-- A single large input exercises validation before any union overlay.
SELECT 'single_corner_touch', length(result), polygonAreaCartesian(result)
FROM
(
    SELECT groupPolygonUnion(p) AS result FROM
    (
        SELECT arrayMap(i -> [[(toFloat64(i), toFloat64(i)), (toFloat64(i), i + 1.),
            (i + 1., i + 1.), (i + 1., toFloat64(i)), (toFloat64(i), toFloat64(i))]], range(2048))::MultiPolygon AS p
    )
);

SELECT 'union_corner_touch', length(result), polygonAreaCartesian(result)
FROM
(
    SELECT groupPolygonUnion([[(toFloat64(number), toFloat64(number)), (toFloat64(number), number + 1.),
        (number + 1., number + 1.), (number + 1., toFloat64(number)), (toFloat64(number), toFloat64(number))]]::Polygon) AS result
    FROM numbers(128)
);

-- All topological checks remain active: overlapping interiors, external holes,
-- nested holes, and disconnected interiors.
SELECT groupPolygonUnion(readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 4,4 4,4 0,0 0)),((2 2,2 6,6 6,6 2,2 2)))')); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonUnion(readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 4,4 4,4 0,0 0),(5 1,6 1,6 2,5 2,5 1)))')); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonUnion(readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 10,10 10,10 0,0 0),(1 1,9 1,9 9,1 9,1 1),(2 2,3 2,3 3,2 3,2 2)))')); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonUnion(readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 4,4 4,4 0,0 0),(0 2,2 1,4 2,2 3,0 2)))')); -- { serverError BAD_ARGUMENTS }
