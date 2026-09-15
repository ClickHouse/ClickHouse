-- Legacy version-1 states can contain up to 16 chunks for `groupPolygonUnion` and 8 for
-- `groupPolygonIntersection`. Readers must keep accepting these boundary states while rejecting
-- a fully valid crafted state with one additional chunk.

SELECT 'union_chunk_count_at_limit';
SELECT round(polygonAreaCartesian(groupPolygonUnionMerge(state)), 2)
FROM
(
    SELECT CAST(unhex(concat(
        '01',  -- version
        '10',  -- 16 chunks
        repeat(substring(hex(groupPolygonUnionState(p)), 5), 16)
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
    FROM
    (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p
    )
);

SELECT 'union_chunk_count_above_limit';
SELECT groupPolygonUnionMerge(state)
FROM
(
    SELECT CAST(unhex(concat(
        '01',  -- version
        '11',  -- 17 chunks
        repeat(substring(hex(groupPolygonUnionState(p)), 5), 17)
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
    FROM
    (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p
    )
); -- { serverError INCORRECT_DATA }

SELECT 'intersect_chunk_count_at_limit';
SELECT round(polygonAreaCartesian(groupPolygonIntersectionMerge(state)), 2)
FROM
(
    SELECT CAST(unhex(concat(
        '01',  -- version
        '01',  -- mode = NonEmpty
        '08',  -- 8 chunks
        repeat(substring(hex(groupPolygonIntersectionState(p)), 7), 8)
    )) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
    FROM
    (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p
    )
);

SELECT 'intersect_chunk_count_above_limit';
SELECT groupPolygonIntersectionMerge(state)
FROM
(
    SELECT CAST(unhex(concat(
        '01',  -- version
        '01',  -- mode = NonEmpty
        '09',  -- 9 chunks
        repeat(substring(hex(groupPolygonIntersectionState(p)), 7), 9)
    )) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
    FROM
    (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p
    )
); -- { serverError INCORRECT_DATA }
