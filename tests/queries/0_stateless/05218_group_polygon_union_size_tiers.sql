SET max_threads = 1;

DROP TABLE IF EXISTS union_tier_inputs;
CREATE TABLE union_tier_inputs (id UInt64, p Polygon) ENGINE = Memory;
INSERT INTO union_tier_inputs
SELECT number, [[
    (3. * number, 0.), (3. * number, 1.), (3. * number + 1., 1.),
    (3. * number + 1., 0.), (3. * number, 0.)]]::Polygon
FROM numbers(1025);

-- Disjoint inputs retain their components across several sizes of accumulated geometry.
SELECT 'disjoint';
SELECT length(result), polygonAreaCartesian(result)
FROM (SELECT groupPolygonUnion(p) AS result FROM union_tier_inputs);

-- Preserve the version-1 representation and round-trip partial states of unequal sizes.
SELECT 'state_merge';
SELECT length(groupPolygonUnionMerge(s)), polygonAreaCartesian(groupPolygonUnionMerge(s))
FROM
(
    SELECT CAST(unhex(hex(groupPolygonUnionState(p))) AS AggregateFunction(groupPolygonUnion, Polygon)) AS s
    FROM union_tier_inputs
    GROUP BY intDiv(id, 65)
);

-- Merging the same right state repeatedly must not consume or mutate it.
SELECT 'reused_right_state';
SELECT length(groupPolygonUnionMerge(s)), polygonAreaCartesian(groupPolygonUnionMerge(s))
FROM
(
    SELECT s, arrayJoin([1, 2, 3]) AS duplicate
    FROM
    (
        SELECT groupPolygonUnionState(p) AS s
        FROM union_tier_inputs
        GROUP BY intDiv(id, 65)
    )
);

-- A window adds more rows after each result; `runningAccumulate` merges after each result.
SELECT 'add_after_finalization';
SELECT countIf(length(result) != id + 1), countIf(polygonAreaCartesian(result) != id + 1)
FROM
(
    SELECT id, groupPolygonUnion(p) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
    FROM union_tier_inputs WHERE id < 33
);

SET allow_deprecated_error_prone_window_functions = 1;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SELECT 'merge_after_finalization';
SELECT sum(length(runningAccumulate(s))), sum(length(finalizeAggregation(s)))
FROM
(
    SELECT id, groupPolygonUnionState(p) AS s
    FROM union_tier_inputs WHERE id < 33
    GROUP BY id ORDER BY id
);

-- Legacy states can contain arbitrary chunk orders and several different point counts.
-- Assemble the old wire representation directly, with the largest chunk first.
SELECT 'legacy_unordered_chunks';
SELECT length(groupPolygonUnionMerge(s)), polygonAreaCartesian(groupPolygonUnionMerge(s))
FROM
(
    SELECT CAST(unhex(concat('0104', arrayStringConcat(arrayReverse(groupArray(chunk)))))
        AS AggregateFunction(groupPolygonUnion, MultiPolygon)) AS s
    FROM
    (
        SELECT number, substring(hex(groupPolygonUnionState(mp)), 5) AS chunk
        FROM
        (
            SELECT number, arrayMap(i -> [[
                (1000. * number + 3. * i, 0.), (1000. * number + 3. * i, 1.),
                (1000. * number + 3. * i + 1., 1.), (1000. * number + 3. * i + 1., 0.),
                (1000. * number + 3. * i, 0.)
            ]], range(toUInt64(pow(4, number))))::MultiPolygon AS mp
            FROM numbers(4)
        )
        GROUP BY number ORDER BY number
    )
);

-- Every open legacy ring gains a closing point before its size is assigned to a tier.
SELECT 'legacy_open_rings';
SELECT length(groupPolygonUnionMerge(s)), polygonAreaCartesian(groupPolygonUnionMerge(s)),
    arraySum(p -> arraySum(r -> length(r), p), groupPolygonUnionMerge(s))
FROM
(
    SELECT CAST(unhex(concat('0110', repeat(concat('0104', substring(chunk, 5, 128), '00'), 16)))
        AS AggregateFunction(groupPolygonUnion, Polygon)) AS s
    FROM
    (
        SELECT substring(hex(groupPolygonUnionState(p)), 5) AS chunk
        FROM union_tier_inputs WHERE id = 0
    )
);

-- A complex containing polygon and small contained polygons can move a union to a lower tier.
TRUNCATE TABLE union_tier_inputs;
INSERT INTO union_tier_inputs SELECT 0,
    [arrayConcat(arrayMap(i -> (toFloat64(i), 0.), range(129)), [(128., 128.), (0., 128.), (0., 0.)])]::Polygon;
INSERT INTO union_tier_inputs
SELECT number + 1, [[
    (2. * number + 1., 1.), (2. * number + 1., 2.), (2. * number + 2., 2.),
    (2. * number + 2., 1.), (2. * number + 1., 1.)]]::Polygon
FROM numbers(32);

SELECT 'containing_polygon_first';
SELECT length(groupPolygonUnion(p)), polygonAreaCartesian(groupPolygonUnion(p))
FROM (SELECT p FROM union_tier_inputs ORDER BY id);
SELECT 'containing_polygon_last';
SELECT length(groupPolygonUnion(p)), polygonAreaCartesian(groupPolygonUnion(p))
FROM (SELECT p FROM union_tier_inputs ORDER BY id DESC);

DROP TABLE union_tier_inputs;
