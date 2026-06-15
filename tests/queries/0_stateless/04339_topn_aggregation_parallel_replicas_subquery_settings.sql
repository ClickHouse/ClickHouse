-- Tags: no-random-settings, no-fasttest

-- Regression test for a contract gap found during review of the TopNAggregation
-- optimization: under parallel replicas, the per-replica partial rewrite mutates the
-- inner plan of `ReadFromLocalParallelReplicaStep`, but that plan executes under the
-- subquery context — the same settings the remote replicas run with. The TopN gates
-- must therefore be derived from that context, not from the initiator's optimization
-- settings: a subquery-level `optimize_topn_aggregation = 0` or `serialize_query_plan = 1`
-- must disable the partial rewrite on the local replica as well, so it follows the same
-- contracts as the remotes.
-- `no-random-settings` keeps the EXPLAIN assertions stable.

DROP TABLE IF EXISTS t_topn_pr_sub;

CREATE TABLE t_topn_pr_sub (grp String, val UInt64)
ENGINE = MergeTree ORDER BY grp;

INSERT INTO t_topn_pr_sub SELECT
    toString(number % 100) AS grp,
    number AS val
FROM numbers(10000);

SET allow_experimental_parallel_reading_from_replicas = 2,
    max_parallel_replicas = 3,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SET serialize_query_plan = 0;
SET optimize_topn_aggregation = 1;
SET topn_aggregation_pruning_level = 2;

-- The partial-only mode appears only with the new analyzer (see 03470).
SET allow_experimental_analyzer = 1;

SELECT '-- sanity: has Partial only true';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT grp, max(val) AS m FROM t_topn_pr_sub
    GROUP BY grp ORDER BY m DESC LIMIT 5
) WHERE explain LIKE '%Partial only: true%';

SELECT '-- subquery disables optimization: no Partial only';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT * FROM (
        SELECT grp, max(val) AS m FROM t_topn_pr_sub
        GROUP BY grp ORDER BY m DESC LIMIT 5
        SETTINGS optimize_topn_aggregation = 0
    )
) WHERE explain LIKE '%Partial only: true%';

SELECT '-- subquery serialize_query_plan: no Partial only';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT * FROM (
        SELECT grp, max(val) AS m FROM t_topn_pr_sub
        GROUP BY grp ORDER BY m DESC LIMIT 5
        SETTINGS serialize_query_plan = 1
    )
) WHERE explain LIKE '%Partial only: true%';

SELECT '-- subquery disabled: correct top-K';
SELECT * FROM (
    SELECT grp, max(val) AS m FROM t_topn_pr_sub
    GROUP BY grp ORDER BY m DESC LIMIT 5
    SETTINGS optimize_topn_aggregation = 0
);

DROP TABLE t_topn_pr_sub;
