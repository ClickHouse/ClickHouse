-- Plan-based parallel replicas distributes a JOIN by splitting one side across replicas and
-- concatenating the per-replica results. That is only correct for some join kinds: INNER (ALL) and LEFT
-- drive the left side, RIGHT drives the right side (SEMI/ANTI ride on the LEFT/RIGHT kind). FULL would
-- duplicate the non-split side's unmatched rows, and CROSS/COMMA/PASTE are also unsafe, so those are kept
-- local. Results must match non-parallel execution. See PR #111063 review (comment r3645282144).

DROP TABLE IF EXISTS jl_04648 SYNC;
DROP TABLE IF EXISTS jr_04648 SYNC;

CREATE TABLE jl_04648 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE jr_04648 (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO jl_04648 SELECT number FROM numbers(2000);          -- 0..1999
INSERT INTO jr_04648 SELECT number + 1000 FROM numbers(2000);   -- 1000..2999 (overlap 1000..1999)

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
-- Pin the plan shape: the local plan must be present to hold the join step, and a randomized join order
-- changes which side is coordinated.
SET parallel_replicas_local_plan = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';

-- For each kind: the row count (equal to non-parallel execution) and the plan steps. The steps show *how*
-- it was distributed, which a plain "is ReadFromParallelReplicas present" check cannot: `Union` first means
-- the whole join shipped as one fragment, `Join` before `ReadFromParallelReplicas` means the join stayed
-- local with only the coordinated read distributed, and no `ReadFromParallelReplicas` means fully local.

SELECT 'INNER', count() FROM jl_04648 INNER JOIN jr_04648 ON jl_04648.id = jr_04648.id;
SELECT 'INNER steps:', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count() FROM jl_04648 INNER JOIN jr_04648 ON jl_04648.id = jr_04648.id)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

SELECT 'LEFT', count() FROM jl_04648 LEFT JOIN jr_04648 ON jl_04648.id = jr_04648.id;
SELECT 'LEFT steps:', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count() FROM jl_04648 LEFT JOIN jr_04648 ON jl_04648.id = jr_04648.id)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

SELECT 'RIGHT', count() FROM jl_04648 RIGHT JOIN jr_04648 ON jl_04648.id = jr_04648.id;
SELECT 'RIGHT steps:', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count() FROM jl_04648 RIGHT JOIN jr_04648 ON jl_04648.id = jr_04648.id)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

SELECT 'LEFT SEMI', count() FROM jl_04648 LEFT SEMI JOIN jr_04648 ON jl_04648.id = jr_04648.id;
SELECT 'LEFT SEMI steps:', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count() FROM jl_04648 LEFT SEMI JOIN jr_04648 ON jl_04648.id = jr_04648.id)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

SELECT 'LEFT ANTI', count() FROM jl_04648 LEFT ANTI JOIN jr_04648 ON jl_04648.id = jr_04648.id;
SELECT 'LEFT ANTI steps:', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count() FROM jl_04648 LEFT ANTI JOIN jr_04648 ON jl_04648.id = jr_04648.id)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

SELECT 'RIGHT SEMI', count() FROM jl_04648 RIGHT SEMI JOIN jr_04648 ON jl_04648.id = jr_04648.id;
SELECT 'RIGHT SEMI steps:', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count() FROM jl_04648 RIGHT SEMI JOIN jr_04648 ON jl_04648.id = jr_04648.id)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

SELECT 'RIGHT ANTI', count() FROM jl_04648 RIGHT ANTI JOIN jr_04648 ON jl_04648.id = jr_04648.id;
SELECT 'RIGHT ANTI steps:', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count() FROM jl_04648 RIGHT ANTI JOIN jr_04648 ON jl_04648.id = jr_04648.id)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

-- A join runtime filter is planted directly above the build side, which for RIGHT is the coordinated side.
-- The split has to be lifted through `BuildRuntimeFilter` as well, otherwise the join would ship only with
-- `enable_join_runtime_filters = 0` - the shape must not depend on that setting.
SELECT 'RIGHT enable_join_runtime_filters=0 steps:', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count() FROM jl_04648 RIGHT JOIN jr_04648 ON jl_04648.id = jr_04648.id)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
) SETTINGS enable_join_runtime_filters = 0;

SELECT 'RIGHT enable_join_runtime_filters=1 steps:', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count() FROM jl_04648 RIGHT JOIN jr_04648 ON jl_04648.id = jr_04648.id)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
) SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 1;

-- FULL must not be distributed and must not duplicate unmatched rows. Checked for both fragment paths.
SELECT 'FULL parallel_replicas_local_plan=1', count() FROM jl_04648 FULL JOIN jr_04648 ON jl_04648.id = jr_04648.id
SETTINGS parallel_replicas_local_plan = 1;
SELECT 'FULL parallel_replicas_local_plan=0', count() FROM jl_04648 FULL JOIN jr_04648 ON jl_04648.id = jr_04648.id
SETTINGS parallel_replicas_local_plan = 0;
SELECT 'FULL steps:', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count() FROM jl_04648 FULL JOIN jr_04648 ON jl_04648.id = jr_04648.id)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

-- CROSS must not be distributed.
SELECT 'CROSS', count() FROM jl_04648 CROSS JOIN jr_04648;
SELECT 'CROSS steps:', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count() FROM jl_04648 CROSS JOIN jr_04648)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

DROP TABLE jl_04648 SYNC;
DROP TABLE jr_04648 SYNC;
