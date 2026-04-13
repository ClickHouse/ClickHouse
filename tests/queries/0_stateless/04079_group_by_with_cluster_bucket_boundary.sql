-- Tests for bucket-boundary correctness in GROUP BY ... WITH CLUSTER
-- These test the bucket optimization: floor(cluster_key / distance) grouping
-- and the adjacent-bucket merging at boundaries.

-- Values exactly at bucket boundary (D=10: values 9 and 10 are in buckets 0 and 1, should merge)
SELECT '--- Bucket boundary merge ---';
SELECT
    min(x) AS mn,
    max(x) AS mx,
    count() AS cnt
FROM (SELECT * FROM VALUES('x UInt64', (5), (9), (10), (15)))
GROUP BY x WITH CLUSTER 10
ORDER BY mn;

-- Values that do NOT merge across bucket boundary (gap > distance)
SELECT '--- Bucket boundary no merge ---';
SELECT
    min(x) AS mn,
    max(x) AS mx,
    count() AS cnt
FROM (SELECT * FROM VALUES('x UInt64', (5), (9), (21), (25)))
GROUP BY x WITH CLUSTER 10
ORDER BY mn;

-- Chain across 3+ buckets (values in buckets 0, 1, 2 that form a connected chain)
SELECT '--- Chain across 3 buckets ---';
SELECT
    min(x) AS mn,
    max(x) AS mx,
    count() AS cnt
FROM (SELECT * FROM VALUES('x UInt64', (1), (9), (11), (19), (21)))
GROUP BY x WITH CLUSTER 10
ORDER BY mn;

-- Chain breaks: two clusters with a gap in the middle
SELECT '--- Chain breaks ---';
SELECT
    min(x) AS mn,
    max(x) AS mx,
    count() AS cnt
FROM (SELECT * FROM VALUES('x UInt64', (1), (9), (11), (50), (55)))
GROUP BY x WITH CLUSTER 10
ORDER BY mn;

-- Values at exact multiples of distance (boundaries between buckets 0|1, 1|2, etc.)
SELECT '--- Exact multiples ---';
SELECT
    min(x) AS mn,
    max(x) AS mx,
    count() AS cnt
FROM (SELECT * FROM VALUES('x UInt64', (0), (10), (20), (30)))
GROUP BY x WITH CLUSTER 10
ORDER BY mn;

-- Same as above but with a gap
SELECT '--- Exact multiples with gap ---';
SELECT
    min(x) AS mn,
    max(x) AS mx,
    count() AS cnt
FROM (SELECT * FROM VALUES('x UInt64', (0), (10), (20), (50)))
GROUP BY x WITH CLUSTER 10
ORDER BY mn;

-- Multiple non-cluster keys with bucket boundary merging
SELECT '--- Multi-key bucket boundary ---';
SELECT
    grp,
    min(x) AS mn,
    max(x) AS mx,
    count() AS cnt
FROM (SELECT * FROM VALUES('grp String, x UInt64',
    ('a', 5), ('a', 9), ('a', 11),
    ('b', 5), ('b', 9), ('b', 50)))
GROUP BY grp, x WITH CLUSTER 10
ORDER BY grp, mn;

-- Distance=1: each integer is its own bucket, adjacent integers merge
SELECT '--- Distance 1 ---';
SELECT
    min(x) AS mn,
    max(x) AS mx,
    count() AS cnt
FROM (SELECT * FROM VALUES('x UInt64', (1), (2), (3), (5), (6), (10)))
GROUP BY x WITH CLUSTER 1
ORDER BY mn;

-- Very small float distance with float keys
SELECT '--- Small float distance ---';
SELECT
    min(x) AS mn,
    max(x) AS mx,
    count() AS cnt
FROM (SELECT * FROM VALUES('x Float64', (1.0), (1.05), (1.1), (2.0), (2.05)))
GROUP BY x WITH CLUSTER 0.1
ORDER BY mn;

-- Distance=0 with duplicates (should behave like regular GROUP BY)
SELECT '--- Distance 0 duplicates ---';
SELECT
    x,
    sum(v) AS total
FROM (SELECT * FROM VALUES('x UInt64, v UInt64', (1, 10), (1, 20), (2, 30), (3, 40), (3, 50)))
GROUP BY x WITH CLUSTER 0
ORDER BY x;

-- Large number of values in same bucket (stress within-bucket merging)
SELECT '--- Many values one bucket ---';
SELECT
    min(number) AS mn,
    max(number) AS mx,
    count() AS cnt,
    sum(number) AS s
FROM numbers(100)
GROUP BY number WITH CLUSTER 1000;

-- Negative values at bucket boundaries
SELECT '--- Negative bucket boundary ---';
SELECT
    min(x) AS mn,
    max(x) AS mx,
    count() AS cnt
FROM (SELECT * FROM VALUES('x Int64', (-15), (-11), (-9), (-5), (0)))
GROUP BY x WITH CLUSTER 10
ORDER BY mn;
