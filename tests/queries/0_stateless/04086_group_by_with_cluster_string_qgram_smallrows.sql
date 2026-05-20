-- Regression: q-gram path missed merges where the Ukkonen lower bound
-- collapses to zero. For Q = 3, max_edits = 1 strings with qgrams <= 3
-- (size <= 5) make the bound `max(qgrams_i, qgrams_j) - Q * max_edits`
-- non-positive, so pairs with zero shared 3-grams could still be within
-- edit distance and must be verified explicitly.
--
-- Setup: 10000 hex hashes of length 16 (pairwise edit distance ~12, no
-- cross-merging) push total_rows past the 10k q-gram threshold; `abc`
-- and `abd` are size-3 strings sharing no 3-gram, yet have edit distance
-- 1 and must merge under `WITH CLUSTER 1`. Without the small-rows
-- fallback they would stay split (3 leftover clusters); with the fix the
-- pair merges (2 leftover clusters total, max size 2).

SELECT count() AS num_clusters, sum(c) AS total_rows, max(c) AS max_cluster_size
FROM (
    SELECT s, count() AS c
    FROM (
        SELECT hex(cityHash64(number)) AS s FROM numbers(10000)
        UNION ALL SELECT 'abc'
        UNION ALL SELECT 'abd'
    )
    GROUP BY s WITH CLUSTER 1
);
