-- Tests for 2D GROUP BY ... WITH CLUSTER <distance>
-- Cluster key is a Tuple(numeric, numeric); points are merged by Euclidean distance.

SELECT '--- Basic 2D: two well-separated clusters ---';
SELECT (x, y) AS p, count() AS c
FROM VALUES('x Float64, y Float64',
    (0.0, 0.0), (0.5, 0.5), (1.0, 0.0),
    (100.0, 100.0), (100.5, 100.5))
GROUP BY (x, y) WITH CLUSTER 1.5
ORDER BY p;

SELECT '--- Same-cell merge (cell side d/sqrt(2) ~= 0.707 for d=1) ---';
SELECT (x, y) AS p, count() AS c
FROM VALUES('x Float64, y Float64',
    (0.1, 0.1), (0.3, 0.3), (0.5, 0.5))
GROUP BY (x, y) WITH CLUSTER 1
ORDER BY p;

SELECT '--- Cross-cell merge: horizontal, vertical, diagonal ---';
SELECT (x, y) AS p, count() AS c
FROM VALUES('x Float64, y Float64',
    -- horizontal pair within d=2
    (0.0, 0.0), (1.5, 0.0),
    -- vertical pair within d=2
    (10.0, 10.0), (10.0, 11.0),
    -- diagonal pair within d=2
    (20.0, 20.0), (21.0, 21.0))
GROUP BY (x, y) WITH CLUSTER 2
ORDER BY p;

SELECT '--- Chain through 3 cells ---';
SELECT (x, y) AS p, count() AS c
FROM VALUES('x Float64, y Float64', (0.0, 0.0), (2.0, 0.0), (4.0, 0.0))
GROUP BY (x, y) WITH CLUSTER 2.5
ORDER BY p;

SELECT '--- No merge: points just over distance d ---';
SELECT (x, y) AS p, count() AS c
FROM VALUES('x Float64, y Float64', (0.0, 0.0), (3.0, 0.0))
GROUP BY (x, y) WITH CLUSTER 2
ORDER BY p;

SELECT '--- d = 0: behaves like plain GROUP BY (x, y) ---';
SELECT (x, y) AS p, count() AS c
FROM VALUES('x Float64, y Float64', (1.0, 1.0), (1.0, 1.0), (2.0, 2.0))
GROUP BY (x, y) WITH CLUSTER 0
ORDER BY p;

SELECT '--- Negative coordinates ---';
SELECT (x, y) AS p, count() AS c
FROM VALUES('x Float64, y Float64', (-5.0, -5.0), (-4.5, -4.5), (5.0, 5.0))
GROUP BY (x, y) WITH CLUSTER 1
ORDER BY p;

SELECT '--- Integer cluster key ---';
SELECT (x, y) AS p, count() AS c
FROM VALUES('x Int32, y Int32', (1, 1), (2, 2), (100, 100))
GROUP BY (x, y) WITH CLUSTER 5
ORDER BY p;

SELECT '--- With non-cluster key ---';
SELECT cat, (x, y) AS p, count() AS c
FROM VALUES('cat String, x Float64, y Float64',
    ('a', 0.0, 0.0), ('a', 0.5, 0.5),
    ('b', 0.0, 0.0), ('b', 100.0, 100.0))
GROUP BY cat, (x, y) WITH CLUSTER 1
ORDER BY cat, p;

SELECT '--- Single row ---';
SELECT (x, y) AS p, count() AS c
FROM VALUES('x Float64, y Float64', (42.0, 42.0))
GROUP BY (x, y) WITH CLUSTER 10
ORDER BY p;

SELECT '--- Multiple aggregate functions ---';
SELECT (x, y) AS p, count() AS c, sum(v) AS s, min(v) AS mn, max(v) AS mx
FROM VALUES('x Float64, y Float64, v Int32',
    (0.0, 0.0, 1), (0.5, 0.5, 2), (1.0, 1.0, 3),
    (100.0, 100.0, 10))
GROUP BY (x, y) WITH CLUSTER 2
ORDER BY p;

SELECT '--- Large d collapses everything ---';
SELECT count() AS num_groups
FROM (
    SELECT (x, y) AS p
    FROM VALUES('x Float64, y Float64',
        (0.0, 0.0), (10.0, 10.0), (50.0, 50.0), (100.0, 100.0))
    GROUP BY (x, y) WITH CLUSTER 1000
);

SELECT '--- EXPLAIN PIPELINE shows ClusterMergingTransform ---';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT (x, y), count()
    FROM VALUES('x Float64, y Float64', (0.0, 0.0))
    GROUP BY (x, y) WITH CLUSTER 1
)
WHERE explain LIKE '%ClusterMerging%';

SELECT '--- EXPLAIN PLAN shows Cluster dimensions: 2 ---';
SELECT count() > 0
FROM (
    EXPLAIN PLAN actions = 1
    SELECT (x, y), count()
    FROM VALUES('x Float64, y Float64', (0.0, 0.0))
    GROUP BY (x, y) WITH CLUSTER 1
)
WHERE explain LIKE '%Cluster dimensions: 2%';
