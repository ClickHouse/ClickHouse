-- `system.one` on the broadcast side of a join has to be serializable for the plan-based parallel
-- replicas implementation to ship the join: the fragment is serialized and rebuilt on each replica,
-- and one non-serializable step keeps the whole fragment local (`liftSplitAboveJoin` then leaves the
-- split below the join, so only the right side's read is distributed).

DROP TABLE IF EXISTS t_one_join SYNC;

CREATE TABLE t_one_join (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_one_join SELECT number FROM numbers(10);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_plan_based = 1,
    -- Without the local plan the fragment runs only on remote replicas, so every check sees a deserialized
    -- read. With it, the initiator's local replica takes the single mark of the table and the remote ones
    -- are cancelled before they read anything.
    parallel_replicas_local_plan = 0;
SET explain_query_plan_default = 'legacy';

-- The join is inside the shipped fragment, so the fragment carries a `JoinLogical` step.
SELECT 'the join is shipped to the replicas';
SELECT countIf(explain ILIKE '%JoinLogical%') FROM (
    EXPLAIN SELECT count(), sum(r.key) FROM system.one AS l RIGHT JOIN t_one_join AS r ON l.dummy = r.key);

-- The `RIGHT JOIN` keeps every row of the table whatever the broadcast side produced. With `join_use_nulls`
-- the unmatched rows get `NULL`, so `count(l.dummy)` counts the rows joined to the single row of `system.one`.
SELECT 'and the answer matches a run without parallel replicas';
SELECT count(), sum(r.key), count(l.dummy) FROM system.one AS l RIGHT JOIN t_one_join AS r ON l.dummy = r.key
SETTINGS join_use_nulls = 1;
SELECT count(), sum(r.key), count(l.dummy) FROM system.one AS l RIGHT JOIN t_one_join AS r ON l.dummy = r.key
SETTINGS enable_parallel_replicas = 0, join_use_nulls = 1;

DROP TABLE t_one_join SYNC;
