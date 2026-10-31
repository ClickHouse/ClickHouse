SET max_threads = 1;

CREATE TEMPORARY TABLE disjoint_holes (id UInt64, p Polygon);
INSERT INTO disjoint_holes
SELECT number, [
    [(0., 0.), (0., 4.), (194., 4.), (194., 0.), (0., 0.)],
    [(3. * number + 1, 1.), (3. * number + 2, 1.), (3. * number + 2, 2.), (3. * number + 1, 2.), (3. * number + 1, 1.)]
] FROM numbers(64);

SELECT 'direct', length(result), length(result[1]) - 1, polygonAreaCartesian(result)
FROM (SELECT groupPolygonIntersection(p) AS result FROM disjoint_holes);

SELECT 'reverse', length(result), length(result[1]) - 1, polygonAreaCartesian(result)
FROM (SELECT groupPolygonIntersection(p) AS result FROM (SELECT * FROM disjoint_holes ORDER BY id DESC));

SELECT 'binary_merge', length(result), length(result[1]) - 1, polygonAreaCartesian(result)
FROM
(
    SELECT groupPolygonIntersectionMerge(s) AS result FROM
    (
        SELECT CAST(unhex(hex(groupPolygonIntersectionState(p))) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS s
        FROM disjoint_holes GROUP BY intDiv(id, 8)
    )
);

-- Repeated/overlapping holes require the general overlay and must not be appended twice.
SELECT 'duplicates', length(result), length(result[1]) - 1, polygonAreaCartesian(result)
FROM (SELECT groupPolygonIntersection(p) AS result FROM disjoint_holes ARRAY JOIN range(2) AS repetition);

-- A changed exterior invalidates the spatial index. A later equal exterior can use it again.
INSERT INTO disjoint_holes VALUES
    (64, [[(0., 0.), (0., 4.), (96., 4.), (96., 0.), (0., 0.)]]),
    (65, [[(0., 0.), (0., 4.), (96., 4.), (96., 0.), (0., 0.)], [(0.25, 1.), (0.5, 1.), (0.5, 2.), (0.25, 2.), (0.25, 1.)]]);

SELECT 'changed_exterior', length(result), length(result[1]) - 1, polygonAreaCartesian(result)
FROM (SELECT groupPolygonIntersection(p) AS result FROM (SELECT * FROM disjoint_holes ORDER BY id));

INSERT INTO disjoint_holes VALUES
    (66, [[(0., 0.), (0., 4.), (96., 4.), (96., 0.), (0., 0.)], [(2., 1.), (2.5, 1.), (2.5, 2.), (2., 2.), (2., 1.)]]);
SELECT 'touching_holes', length(result), length(result[1]) - 1, polygonAreaCartesian(result)
FROM (SELECT groupPolygonIntersection(p) AS result FROM (SELECT * FROM disjoint_holes ORDER BY id));

-- Empty must still be discovered before the following invalid polygon is validated.
INSERT INTO disjoint_holes VALUES
    (67, [[(1000., 0.), (1000., 1.), (1001., 1.), (1001., 0.), (1000., 0.)]]),
    (68, [[(0., 0.), (2., 2.), (0., 2.), (2., 0.), (0., 0.)]]);
SELECT 'early_empty', empty(groupPolygonIntersection(p)) FROM (SELECT * FROM disjoint_holes ORDER BY id);

-- Matching exterior alone cannot bypass validation of a new hole outside it.
SELECT groupPolygonIntersection(p) FROM
(
    SELECT arrayJoin([
        [[(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]],
        [[(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)], [(5., 1.), (6., 1.), (6., 2.), (5., 2.), (5., 1.)]]
    ])::Polygon AS p
); -- { serverError BAD_ARGUMENTS }
