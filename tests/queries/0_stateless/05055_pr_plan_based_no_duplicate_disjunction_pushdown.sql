-- Tags: no-old-analyzer
-- The plan-based parallel-replicas path and `JoinStepLogical` exist only in the new analyzer.

-- Join reordering rebuilds the join steps, which used to reset the guard that says the disjunction
-- push-down already ran. A plan optimized twice - which is what the plan-based parallel-replicas path
-- does - then pushed the same partial predicate a second time, leaving a `Filter` above a read whose
-- PREWHERE already applies it. The plan must not depend on `parallel_replicas_plan_based`.

DROP TABLE IF EXISTS disj_left;
DROP TABLE IF EXISTS disj_right;

CREATE TABLE disj_left (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
INSERT INTO disj_left SELECT number, if(number % 2 = 0, 'FRANCE', 'GERMANY') FROM numbers(1000);

CREATE TABLE disj_right (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
INSERT INTO disj_right SELECT number, if(number % 3 = 0, 'FRANCE', 'GERMANY') FROM numbers(1000);

SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
SET use_join_disjunctions_push_down = 1;

-- The pushed disjunction must appear once per read - as a `Filter` or in `PREWHERE`, not as both.
-- Before the fix the plan-based build pushed it a second time and left a redundant `Filter` above a
-- read whose `PREWHERE` already applied it, which showed up here as four occurrences instead of two.
-- The second column names the parallel-replicas read each mode is expected to build, so a query that
-- silently fell back to a local plan fails here instead of comparing two plans that prove nothing.
SELECT
    countIf(explain LIKE '%FRANCE%' AND explain LIKE '%OR%'
            AND (explain LIKE '%Filter column:%' OR explain LIKE '%Prewhere filter column:%')) AS pushed_disjunctions,
    countIf(explain LIKE '%ReadFromRemoteParallelReplicas%') AS tree_based_reads
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM disj_left AS l, disj_right AS r
    WHERE l.k = r.k AND ((l.name = 'FRANCE' AND r.name = 'GERMANY') OR (l.name = 'GERMANY' AND r.name = 'FRANCE'))
)
SETTINGS parallel_replicas_plan_based = 0;

SELECT
    countIf(explain LIKE '%FRANCE%' AND explain LIKE '%OR%'
            AND (explain LIKE '%Filter column:%' OR explain LIKE '%Prewhere filter column:%')) AS pushed_disjunctions,
    countIf(explain LIKE '%ReadFromParallelReplicas (QueryPlan%') AS plan_based_reads
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM disj_left AS l, disj_right AS r
    WHERE l.k = r.k AND ((l.name = 'FRANCE' AND r.name = 'GERMANY') OR (l.name = 'GERMANY' AND r.name = 'FRANCE'))
)
SETTINGS parallel_replicas_plan_based = 1;

-- And the answer does not change.
SELECT count() FROM disj_left AS l, disj_right AS r
WHERE l.k = r.k AND ((l.name = 'FRANCE' AND r.name = 'GERMANY') OR (l.name = 'GERMANY' AND r.name = 'FRANCE'))
SETTINGS parallel_replicas_plan_based = 0;

SELECT count() FROM disj_left AS l, disj_right AS r
WHERE l.k = r.k AND ((l.name = 'FRANCE' AND r.name = 'GERMANY') OR (l.name = 'GERMANY' AND r.name = 'FRANCE'))
SETTINGS parallel_replicas_plan_based = 1;

DROP TABLE disj_left;
DROP TABLE disj_right;
