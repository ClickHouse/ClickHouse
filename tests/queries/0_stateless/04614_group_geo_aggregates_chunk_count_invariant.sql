-- A `groupPolygonUnion` state can contain up to 16 chunks, and a non-empty `groupPolygonIntersection`
-- state exactly one. Readers must accept these boundary states and reject a fully valid crafted
-- state with one additional chunk.

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

SELECT 'intersect_chunk_count_one';
SELECT round(polygonAreaCartesian(groupPolygonIntersectionMerge(state)), 2)
FROM
(
    SELECT CAST(unhex(concat(
        '01',  -- version
        '01',  -- mode = NonEmpty
        '01',  -- 1 chunk
        substring(hex(groupPolygonIntersectionState(p)), 7)
    )) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
    FROM
    (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p
    )
);

SELECT 'intersect_chunk_count_two';
SELECT groupPolygonIntersectionMerge(state)
FROM
(
    SELECT CAST(unhex(concat(
        '01',  -- version
        '01',  -- mode = NonEmpty
        '02',  -- 2 chunks
        repeat(substring(hex(groupPolygonIntersectionState(p)), 7), 2)
    )) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
    FROM
    (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p
    )
); -- { serverError INCORRECT_DATA }
