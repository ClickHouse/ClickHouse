-- Tags: no-random-settings, no-random-merge-tree-settings

-- The initiator's share of a parallel-replicas read (`parallel_replicas_local_plan = 1`) is an
-- unoptimized fragment held by `ReadFromLocalParallelReplicaStep`; it is optimized separately,
-- in the second pass of `optimizeTreeSecondPass`. That pass keeps the *outer* optimization
-- settings and overrides only a hand-picked list with the fragment's own subquery settings.
--
-- `query_plan_push_limit_by_into_sort` must be on that list. `pushLimitByIntoSort` is what
-- attaches the `LIMIT BY` hint to the `SortingStep`, and `optimizeReadInOrder` reads that hint
-- to decide whether the per-part `PrefetchingConcat` has to be given up in favour of multiple
-- streams (`preferMultipleStreamsForPushedDownLimitBy`). The remote replicas re-optimize the
-- shipped fragment under the subquery's own settings, so if the initiator used the outer value
-- instead, a subquery-scoped `SETTINGS query_plan_push_limit_by_into_sort` would shape the local
-- fragment differently from the remote ones.
--
-- The anchor is the per-stream `LimitBySortedStreamTransform × N` prefilter: the local fragment
-- lives below the `(Union)` that joins it with `(ReadFromRemoteParallelReplicas)`, so a
-- multi-stream prefilter appearing *after* the `(Union)` line belongs to the local fragment,
-- while one appearing before it belongs to the outer plan.

DROP TABLE IF EXISTS t_limit_by_push_down_pr_local;

CREATE TABLE t_limit_by_push_down_pr_local (grp UInt64, key UInt64, value String)
ENGINE = MergeTree ORDER BY (grp, key)
SETTINGS index_granularity = 1024;

SYSTEM STOP MERGES t_limit_by_push_down_pr_local;

INSERT INTO t_limit_by_push_down_pr_local SELECT number % 100, number, toString(number) FROM numbers(30000);
INSERT INTO t_limit_by_push_down_pr_local SELECT number % 100, number, toString(number) FROM numbers(30000, 30000);
INSERT INTO t_limit_by_push_down_pr_local SELECT number % 100, number, toString(number) FROM numbers(60000, 30000);

SELECT 'parts';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_limit_by_push_down_pr_local' AND active;

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
SET optimize_read_in_order = 1;
SET max_threads = 6;

-- The outer query disables the pushdown, the shipped subquery enables it: the local fragment
-- must follow the subquery, i.e. carry the per-stream prefilter below the `(Union)`.
-- Before the fix the local fragment inherited the outer `0` and had no prefilter at all.
SET query_plan_push_limit_by_into_sort = 0;

SELECT 'subquery_enables_pushdown';
SELECT
    countIf(explain LIKE '%(Union)%') = 1,
    maxIf(rn, explain LIKE '%LimitBySortedStreamTransform × %') > minIf(rn, explain LIKE '%(Union)%'),
    countIf(explain LIKE '%MergeTreeSelect(pool: ReadPoolParallelReplicasInOrder%') = 1
FROM
(
    SELECT rowNumberInAllBlocks() AS rn, explain
    FROM (EXPLAIN PIPELINE SELECT * FROM (
        SELECT * FROM t_limit_by_push_down_pr_local ORDER BY grp, key LIMIT 3 BY grp
        SETTINGS query_plan_push_limit_by_into_sort = 1
    ))
);

-- The mirror image: the outer query enables the pushdown, the shipped subquery disables it.
-- The local fragment must have no per-stream prefilter, even though the outer plan does.
SET query_plan_push_limit_by_into_sort = 1;

SELECT 'subquery_disables_pushdown';
SELECT
    countIf(explain LIKE '%(Union)%') = 1,
    maxIf(rn, explain LIKE '%LimitBySortedStreamTransform × %') < minIf(rn, explain LIKE '%(Union)%'),
    countIf(explain LIKE '%MergeTreeSelect(pool: ReadPoolParallelReplicasInOrder%') = 1
FROM
(
    SELECT rowNumberInAllBlocks() AS rn, explain
    FROM (EXPLAIN PIPELINE SELECT * FROM (
        SELECT * FROM t_limit_by_push_down_pr_local ORDER BY grp, key LIMIT 3 BY grp
        SETTINGS query_plan_push_limit_by_into_sort = 0
    ))
);

-- Whatever the fragment is optimized under, the answer must not change.
SELECT 'correctness';
SELECT count(), sum(grp), sum(key) FROM (
    SELECT * FROM (
        SELECT grp, key FROM t_limit_by_push_down_pr_local ORDER BY grp, key LIMIT 3 BY grp
        SETTINGS query_plan_push_limit_by_into_sort = 1
    )
);
SELECT count(), sum(grp), sum(key) FROM (
    SELECT * FROM (
        SELECT grp, key FROM t_limit_by_push_down_pr_local ORDER BY grp, key LIMIT 3 BY grp
        SETTINGS query_plan_push_limit_by_into_sort = 0
    )
);

DROP TABLE t_limit_by_push_down_pr_local;
