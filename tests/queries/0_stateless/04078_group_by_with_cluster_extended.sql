-- Extended tests for GROUP BY ... WITH CLUSTER <distance>

-- Float distance with integer keys
SELECT '--- Float distance ---';
SELECT
    min(ts) AS cluster_start,
    max(ts) AS cluster_end,
    count() AS cnt
FROM (SELECT * FROM VALUES('ts UInt64', (1), (2), (4), (5), (10)))
GROUP BY ts WITH CLUSTER 1.5
ORDER BY cluster_start;

-- Float column as cluster key
SELECT '--- Float cluster key ---';
SELECT
    min(val) AS mn,
    max(val) AS mx,
    count() AS cnt
FROM (SELECT * FROM VALUES('val Float64', (1.0), (1.3), (1.7), (5.0), (5.4)))
GROUP BY val WITH CLUSTER 0.5
ORDER BY mn;

-- Negative values in cluster key
SELECT '--- Negative values ---';
SELECT
    min(x) AS mn,
    max(x) AS mx,
    count() AS cnt
FROM (SELECT * FROM VALUES('x Int64', (-10), (-8), (-5), (0), (2)))
GROUP BY x WITH CLUSTER 3
ORDER BY mn;

-- Single row input
SELECT '--- Single row ---';
SELECT min(ts), max(ts), sum(v)
FROM (SELECT * FROM VALUES('ts UInt64, v UInt64', (42, 100)))
GROUP BY ts WITH CLUSTER 10;

-- Empty input
SELECT '--- Empty input ---';
SELECT min(ts), max(ts), sum(v)
FROM (SELECT * FROM VALUES('ts UInt64, v UInt64', (1, 1)) WHERE 0)
GROUP BY ts WITH CLUSTER 10;

-- Multiple aggregate functions (avg, count, sum, min, max, any)
SELECT '--- Multiple aggregates ---';
SELECT
    min(ts) AS cl_start,
    max(ts) AS cl_end,
    count() AS cnt,
    sum(v) AS s,
    avg(v) AS a
FROM (SELECT * FROM VALUES('ts UInt64, v UInt64',
    (1, 10), (2, 20), (3, 30),
    (100, 40), (101, 50)))
GROUP BY ts WITH CLUSTER 5
ORDER BY cl_start;

-- Cluster key is NOT the last GROUP BY key (first key has WITH CLUSTER)
SELECT '--- Cluster on first key ---';
SELECT
    min(ts) AS cl_start,
    max(ts) AS cl_end,
    category,
    sum(v) AS total
FROM (SELECT * FROM VALUES('ts UInt64, category String, v UInt64',
    (1, 'a', 10), (3, 'a', 20), (1, 'b', 30), (2, 'b', 40)))
GROUP BY ts WITH CLUSTER 5, category
ORDER BY category, cl_start;

-- Large distance: everything collapses into one group per non-cluster key
SELECT '--- Large distance ---';
SELECT
    user_id,
    count() AS cnt,
    sum(v) AS total
FROM (SELECT * FROM VALUES('user_id String, ts UInt64, v UInt64',
    ('a', 1, 1), ('a', 1000000, 2), ('b', 1, 3), ('b', 999999, 4)))
GROUP BY user_id, ts WITH CLUSTER 99999999
ORDER BY user_id;

-- Duplicate values in cluster key
SELECT '--- Duplicate cluster keys ---';
SELECT
    min(ts) AS cl_start,
    max(ts) AS cl_end,
    sum(v) AS total
FROM (SELECT * FROM VALUES('ts UInt64, v UInt64',
    (5, 10), (5, 20), (5, 30), (100, 40)))
GROUP BY ts WITH CLUSTER 1
ORDER BY cl_start;

-- EXPLAIN PIPELINE: verify ClusterMerging step appears
SELECT '--- EXPLAIN PIPELINE ---';
EXPLAIN PIPELINE header=0 SELECT sum(number) FROM numbers(100) GROUP BY number % 10 WITH CLUSTER 5 SETTINGS max_threads=1;

-- Correctness: compare result count with regular GROUP BY
-- With distance=0, cluster GROUP BY should produce same number of rows
SELECT '--- Correctness: count with distance=0 ---';
SELECT
    (SELECT count() FROM (SELECT number % 1000 AS k FROM numbers(10000) GROUP BY k))
    =
    (SELECT count() FROM (SELECT number % 1000 AS k, sum(1) FROM numbers(10000) GROUP BY k WITH CLUSTER 0));

-- Verify aggregation correctness on larger dataset:
-- sum should match regardless of clustering
SELECT '--- Correctness: sum invariant ---';
SELECT
    (SELECT sum(v) FROM (
        SELECT number % 100 AS k, number AS v FROM numbers(10000)
    ) GROUP BY k WITH CLUSTER 5)
    =
    (SELECT sum(number) FROM numbers(10000));

-- Int32 key type
SELECT '--- Int32 cluster key ---';
SELECT min(k), max(k), count()
FROM (SELECT * FROM VALUES('k Int32', (1), (2), (10), (11)))
GROUP BY k WITH CLUSTER 2
ORDER BY min(k);

-- Three keys: two non-cluster + one cluster
SELECT '--- Three GROUP BY keys ---';
SELECT a, b, min(ts), max(ts), count()
FROM (SELECT * FROM VALUES('a UInt8, b UInt8, ts UInt64, v UInt64',
    (1, 1, 10, 1), (1, 1, 12, 1), (1, 1, 50, 1),
    (1, 2, 10, 1), (1, 2, 11, 1),
    (2, 1, 10, 1)))
GROUP BY a, b, ts WITH CLUSTER 5
ORDER BY a, b, min(ts);
