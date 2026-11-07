-- Regression: a matcher (`*`, `COLUMNS(...)`) or an expanding function before the
-- `WITH CLUSTER` key changes the number of GROUP BY elements during analysis, so the
-- parser-side cluster-key index no longer points at the cluster key in the resolved
-- list. The analyzer used to keep that stale index and the planner then failed with a
-- `LOGICAL_ERROR` ("Failed to resolve GROUP BY ... WITH CLUSTER key names"). The
-- cluster key must be tracked through expansion instead.

SET enable_analyzer = 1; -- `WITH CLUSTER` is implemented for the analyzer only
SET allow_experimental_group_by_with_cluster = 1;

-- A matcher that matches nothing must behave exactly like omitting it.
SELECT 'matcher matches nothing';
SELECT
(
    SELECT count() FROM (SELECT count() FROM VALUES('x UInt64', (1), (2), (100), (101)) GROUP BY COLUMNS('nomatch'), x WITH CLUSTER 1)
) = (
    SELECT count() FROM (SELECT count() FROM VALUES('x UInt64', (1), (2), (100), (101)) GROUP BY x WITH CLUSTER 1)
);

-- A matcher matching a real non-cluster key must behave like naming it explicitly.
SELECT 'matcher matches a real column';
SELECT
(
    SELECT count() FROM (SELECT count() FROM VALUES('a UInt64, x UInt64', (5, 1), (5, 2), (5, 100), (7, 1)) GROUP BY COLUMNS('a'), x WITH CLUSTER 1)
) = (
    SELECT count() FROM (SELECT count() FROM VALUES('a UInt64, x UInt64', (5, 1), (5, 2), (5, 100), (7, 1)) GROUP BY a, x WITH CLUSTER 1)
);

-- The exact shape found by the AST fuzzer (matcher matches nothing, huge distance
-- collapses every key into one cluster) must no longer raise a logical error.
SELECT 'fuzzer shape runs';
SELECT count() AS num_clusters FROM (
    SELECT count() FROM VALUES('ts UInt64, value UInt64', (1, 10), (65536, 2), (2, 3), (100, 40), (105, 50), (257, 2))
    GROUP BY COLUMNS('1$'), ts WITH CLUSTER 9223372036854775806
);

-- A matcher may not stand in for the cluster key itself: it must resolve to a single
-- expression. Here the matcher used as the cluster key matches two columns.
SELECT 'matcher as cluster key rejected';
SELECT count() FROM VALUES('c1 UInt64, c2 UInt64', (1, 2)) GROUP BY COLUMNS('c') WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }
