-- Test GROUP BY ... WITH CLUSTER <distance> modifier

-- Basic test: cluster timestamps within distance 10
SELECT
    min(ts) AS cluster_start,
    max(ts) AS cluster_end,
    sum(value) AS total
FROM
(
    SELECT * FROM VALUES('ts UInt64, value UInt64',
        (1, 10), (5, 20), (8, 30),
        (100, 40), (105, 50),
        (200, 60))
)
GROUP BY ts WITH CLUSTER 10
ORDER BY cluster_start;

-- Multiple keys: cluster by ts within distance, grouped by user
SELECT
    user_id,
    min(ts) AS cluster_start,
    max(ts) AS cluster_end,
    sum(value) AS total
FROM
(
    SELECT * FROM VALUES('user_id String, ts UInt64, value UInt64',
        ('alice', 1, 10), ('alice', 5, 20), ('alice', 100, 30),
        ('bob', 1, 40), ('bob', 2, 50), ('bob', 200, 60))
)
GROUP BY user_id, ts WITH CLUSTER 10
ORDER BY user_id, cluster_start;

-- Distance = 0 should behave like regular GROUP BY
SELECT
    ts,
    sum(value) AS total
FROM
(
    SELECT * FROM VALUES('ts UInt64, value UInt64',
        (1, 10), (1, 20), (2, 30), (2, 40))
)
GROUP BY ts WITH CLUSTER 0
ORDER BY ts;

-- Single group: all values within distance
SELECT
    min(ts) AS cluster_start,
    max(ts) AS cluster_end,
    count() AS cnt
FROM
(
    SELECT * FROM VALUES('ts UInt64',
        (1), (3), (5), (7), (9))
)
GROUP BY ts WITH CLUSTER 5
ORDER BY cluster_start;

-- Each row is its own cluster (distance too small)
SELECT
    ts,
    count() AS cnt
FROM
(
    SELECT * FROM VALUES('ts UInt64',
        (1), (100), (200))
)
GROUP BY ts WITH CLUSTER 1
ORDER BY ts;

-- Chain semantics: 1, 6, 11 form one cluster (1->6 within 5, 6->11 within 5)
-- but 1 and 11 differ by 10 > 5
SELECT
    min(ts) AS cluster_start,
    max(ts) AS cluster_end,
    count() AS cnt
FROM
(
    SELECT * FROM VALUES('ts UInt64',
        (1), (6), (11), (100))
)
GROUP BY ts WITH CLUSTER 5
ORDER BY cluster_start;

-- Parser round-trip: EXPLAIN AST
SELECT '--- EXPLAIN AST ---';
EXPLAIN AST SELECT sum(x) FROM numbers(10) GROUP BY number, x WITH CLUSTER 42;
