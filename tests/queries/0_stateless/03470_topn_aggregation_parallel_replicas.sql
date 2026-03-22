-- Tags: no-random-settings, no-fasttest

-- TopN aggregation merge-only mode under parallel replicas.
-- Verifies that the optimizer replaces Limit → Sorting → MergingAggregated
-- with TopNAggregating(merge_only) when parallel replicas are enabled.

DROP TABLE IF EXISTS t_topn_pr;

CREATE TABLE t_topn_pr (grp String, val UInt64)
ENGINE = MergeTree ORDER BY grp;

INSERT INTO t_topn_pr SELECT
    toString(number % 100) AS grp,
    number AS val
FROM numbers(10000);

SET allow_experimental_parallel_reading_from_replicas = 2,
    max_parallel_replicas = 3,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- EXPLAIN: verify TopNAggregating with merge_only appears under parallel replicas
SELECT '-- EXPLAIN: has TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT grp, max(val) AS m FROM t_topn_pr
    GROUP BY grp ORDER BY m DESC LIMIT 5
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%TopNAggregating%';

-- merge_only / partial_only modes only appear with the new analyzer because the
-- old analyzer builds a different plan structure for parallel replicas (no
-- MergingAggregatedStep), so force the new analyzer at the session level for
-- these EXPLAIN checks (setting it inside a subquery is disallowed when the
-- top-level value differs).
SET allow_experimental_analyzer = 1;

SELECT '-- EXPLAIN: has Merge only true';
SELECT count() > 0 FROM (
    EXPLAIN actions=1
    SELECT grp, max(val) AS m FROM t_topn_pr
    GROUP BY grp ORDER BY m DESC LIMIT 5
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%Merge only: true%';

SELECT '-- EXPLAIN: has Partial only true';
SELECT count() > 0 FROM (
    EXPLAIN actions=1
    SELECT grp, max(val) AS m FROM t_topn_pr
    GROUP BY grp ORDER BY m DESC LIMIT 5
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%Partial only: true%';

SET allow_experimental_analyzer = 0;

SELECT '-- EXPLAIN: has Threshold pruning true';
SELECT count() > 0 FROM (
    EXPLAIN actions=1
    SELECT grp, max(val) AS m FROM t_topn_pr
    GROUP BY grp ORDER BY m DESC LIMIT 5
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%Threshold pruning: true%';

-- Correctness: max DESC
SELECT '-- max DESC: optimized (parallel replicas)';
SELECT grp, max(val) AS m FROM t_topn_pr
GROUP BY grp ORDER BY m DESC LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- max DESC: reference';
SELECT grp, max(val) AS m FROM t_topn_pr
GROUP BY grp ORDER BY m DESC LIMIT 5
SETTINGS optimize_topn_aggregation = 0;

-- Correctness: min ASC
SELECT '-- min ASC: optimized (parallel replicas)';
SELECT grp, min(val) AS m FROM t_topn_pr
GROUP BY grp ORDER BY m ASC LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- min ASC: reference';
SELECT grp, min(val) AS m FROM t_topn_pr
GROUP BY grp ORDER BY m ASC LIMIT 5
SETTINGS optimize_topn_aggregation = 0;

-- Correctness: max with any companion
SELECT '-- max + any: optimized (parallel replicas)';
SELECT grp, max(val) AS m, any(grp) AS g FROM t_topn_pr
GROUP BY grp ORDER BY m DESC LIMIT 3
SETTINGS optimize_topn_aggregation = 1;

-- Edge case: LIMIT larger than number of groups
SELECT '-- K larger than groups';
SELECT grp, max(val) AS m FROM t_topn_pr
GROUP BY grp ORDER BY m DESC LIMIT 200
SETTINGS optimize_topn_aggregation = 1;

-- Negative case: optimization disabled should still produce correct results
SELECT '-- disabled: no TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT grp, max(val) AS m FROM t_topn_pr
    GROUP BY grp ORDER BY m DESC LIMIT 5
    SETTINGS optimize_topn_aggregation = 0
) WHERE explain LIKE '%TopNAggregating%';

DROP TABLE t_topn_pr;
