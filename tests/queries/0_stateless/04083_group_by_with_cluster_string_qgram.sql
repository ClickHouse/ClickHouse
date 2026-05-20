-- Tests that exercise the q-gram inverted index fast-path in `WITH CLUSTER`
-- for String keys (active for `total_rows >= 10000`).
-- Each test produces a result identical to what a naive O(N^2) sweep would
-- give; the test contract is correctness, not the path taken.

SELECT '--- 12k random hex strings, d=1 ---';
-- 16-char hex of cityHash64 — uniformly distributed strings, pairwise edit
-- distance is typically >= 12. With d=1 nothing merges; expected 12000 clusters.
SELECT count() AS num_groups
FROM (
    SELECT s
    FROM (SELECT hex(cityHash64(number)) AS s FROM numbers(12000))
    GROUP BY s WITH CLUSTER 1
);

SELECT '--- 100 well-separated clusters of 120 random variants each, d=2 ---';
-- 100 distinct hex prefixes of length 16; for each, 120 rows with the SAME
-- string (after standard GROUP BY collapses to one row per prefix).
-- Each prefix is well-separated from all others (typical edit distance > 12).
-- Expected: 100 clusters totalling 12000 rows.
SELECT count() AS num_groups, sum(c) AS total
FROM (
    SELECT s, count() AS c
    FROM (
        SELECT hex(cityHash64(intDiv(number, 120))) AS s
        FROM numbers(12000)
    )
    GROUP BY s WITH CLUSTER 2
);

SELECT '--- Naive vs q-gram path: identical input via modulo, identical answer ---';
-- Both subqueries see exactly the same set of distinct strings (200 unique),
-- but different row counts move them across the q-gram threshold (10000):
--   N = 9000  → naive O(N^2) path
--   N = 11000 → q-gram inverted-index path
-- The cluster count must be identical.
-- Nested `SELECT s FROM (SELECT hex(...) AS s ...)` keeps the cluster key
-- a String — without the extra level the planner notices that `count()`
-- doesn't use `s`, strips `hex()`, and the cluster step would receive the
-- UInt64 `cityHash64` result instead.
SELECT
    (SELECT count() FROM (SELECT s FROM (SELECT hex(cityHash64(number % 200)) AS s
                                         FROM numbers(9000))
                          GROUP BY s WITH CLUSTER 2)) AS naive_clusters,
    (SELECT count() FROM (SELECT s FROM (SELECT hex(cityHash64(number % 200)) AS s
                                         FROM numbers(11000))
                          GROUP BY s WITH CLUSTER 2)) AS qgram_clusters;

SELECT '--- 20k strings, half cluster A, half cluster B, gap > d ---';
-- 10k variants of 'apple' (within d=1 of each other) + 10k of 'orange' (within d=1).
-- Distance between 'apple' and 'orange' is large, so two separate clusters.
SELECT count() AS num_groups, sum(c) AS total
FROM (
    SELECT s, count() AS c
    FROM (
        SELECT
            if(number % 2 = 0, 'apple_' || toString(intDiv(number, 2) % 5),
                                'orange_' || toString(intDiv(number, 2) % 5)) AS s
        FROM numbers(20000)
    )
    GROUP BY s WITH CLUSTER 1
);

SELECT '--- 10k well-separated + 1 short string at the end ---';
-- Verifies the short-string fallback path: 10000 long strings + 1 single-char
-- string that does not match any of them within d=1.
SELECT count() AS num_groups
FROM (
    SELECT s
    FROM (
        SELECT 'long_string_' || toString(number) AS s FROM numbers(10000)
        UNION ALL SELECT 'a'
    )
    GROUP BY s WITH CLUSTER 1
);
