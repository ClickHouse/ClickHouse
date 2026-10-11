SET max_threads = 1;

CREATE TEMPORARY TABLE hull_wire_state (s AggregateFunction(groupConvexHull, Point));
INSERT INTO hull_wire_state
SELECT groupConvexHullState((toFloat64(number % 100), toFloat64(intDiv(number, 100)))::Point) FROM numbers(10000);

-- Four corners, not the 10,000-point backlog. Repeated serialization is stable,
-- and a true binary round trip keeps exactly the same hull.
SELECT 'compact_grid', length(unhex(hex(s))), startsWith(hex(s), '030404'),
    hex(s) = hex(CAST(unhex(hex(s)) AS AggregateFunction(groupConvexHull, Point))),
    arraySort(arrayDistinct(finalizeAggregation(s))) = [(0., 0.), (0., 99.), (99., 0.), (99., 99.)]
FROM hull_wire_state;

SELECT 'merge_after_serialization', arraySort(arrayDistinct(groupConvexHullMerge(s))) = [(0., 0.), (0., 99.), (99., 99.), (200., 0.)]
FROM
(
    SELECT CAST(unhex(hex(s)) AS AggregateFunction(groupConvexHull, Point)) AS s FROM hull_wire_state
    UNION ALL
    SELECT groupConvexHullState((200., 0.)::Point) AS s
);

-- An untrusted watermark claiming that all five points are a hull must not let an
-- interior point survive compaction. Coordinates are encoded from a plain point array.
SELECT 'untrusted_prefix', length(unhex(hex(s))) = 67, length(arrayDistinct(finalizeAggregation(s))) = 4
FROM
(
    SELECT CAST(unhex(concat('030505',
        '00000000000000000000000000000000',
        '00000000000000000000000000001040',
        '00000000000010400000000000001040',
        '00000000000010400000000000000000',
        '00000000000000400000000000000040')) AS AggregateFunction(groupConvexHull, Point)) AS s
);

-- Small partial states are compact before transfer and remain bounded while merging.
SELECT 'many_partial_states', max(bytes) = 67, length(arrayDistinct(groupConvexHullMerge(s))) = 4
FROM
(
    SELECT CAST(unhex(encoded) AS AggregateFunction(groupConvexHull, Point)) AS s, length(unhex(encoded)) AS bytes
    FROM
    (
        SELECT intDiv(number, 10000) AS part,
            hex(groupConvexHullState((toFloat64(number % 100), toFloat64(intDiv(number % 10000, 100)))::Point)) AS encoded
        FROM numbers(1000000) GROUP BY part
    )
);
