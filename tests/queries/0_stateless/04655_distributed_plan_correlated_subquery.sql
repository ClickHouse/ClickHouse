-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- make_distributed_plan auto-disables correlated_subqueries_use_in_memory_buffer (issue #109476):
-- the in-memory buffer for a decorrelated subquery is process-local and its steps are not
-- serializable, so a multi-stage distributed plan used to fail with SUPPORT_IS_DISABLED
-- ("step 'SaveSubqueryResultToBuffer' ... is not serializable for remote execution").

DROP TABLE IF EXISTS t_corr_big;
DROP TABLE IF EXISTS t_corr_small;

CREATE TABLE t_corr_big (id UInt64, grp UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_corr_small (grp UInt64, val UInt64) ENGINE = MergeTree ORDER BY grp;
INSERT INTO t_corr_big SELECT number, number % 100 FROM numbers(100000);
INSERT INTO t_corr_small SELECT number * 2, number FROM numbers(50);

-- The buffer setting is pinned to its default (on) so the bug path is provably exercised;
-- the auto-switch disables it for distributed plans regardless.
-- correlated_subqueries_substitute_equivalent_expressions is pinned off: with the substitution
-- the decorrelated plan has no common subplan and the buffer is never engaged.
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_default_shuffle_join_bucket_count = 3, max_rows_to_group_by = 0,
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1,
    correlated_subqueries_substitute_equivalent_expressions = 0;

SELECT 'correlated scalar aggregate subquery works under make_distributed_plan';
SELECT count() FROM t_corr_big AS o WHERE o.id < (SELECT avg(i.id) FROM t_corr_big AS i WHERE i.grp = o.grp);
SELECT count() FROM t_corr_big AS o WHERE o.id < (SELECT avg(i.id) FROM t_corr_big AS i WHERE i.grp = o.grp)
    SETTINGS make_distributed_plan = 0;

SELECT 'correlated EXISTS works under make_distributed_plan';
SELECT count() FROM t_corr_big AS o WHERE EXISTS (SELECT 1 FROM t_corr_small AS s WHERE s.grp = o.grp);
SELECT count() FROM t_corr_big AS o WHERE EXISTS (SELECT 1 FROM t_corr_small AS s WHERE s.grp = o.grp)
    SETTINGS make_distributed_plan = 0;

SELECT 'the query distributes';
SELECT 'distributes'
FROM (EXPLAIN PIPELINE SELECT count() FROM t_corr_big AS o
      WHERE o.id < (SELECT avg(i.id) FROM t_corr_big AS i WHERE i.grp = o.grp))
WHERE explain LIKE '%ReadFromDistributedPlanSource%' LIMIT 1;

DROP TABLE t_corr_big;
DROP TABLE t_corr_small;
