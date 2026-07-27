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

-- For each kind: the row count (equal to non-parallel execution) and whether it was distributed.
-- INNER / LEFT / RIGHT / SEMI / ANTI are distributed (has_remote_read = 1); FULL / CROSS are not (0).

SELECT 'INNER', count() FROM jl_04648 INNER JOIN jr_04648 ON jl_04648.id = jr_04648.id;
SELECT 'INNER remote', countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM jl_04648 INNER JOIN jr_04648 ON jl_04648.id = jr_04648.id);

SELECT 'LEFT', count() FROM jl_04648 LEFT JOIN jr_04648 ON jl_04648.id = jr_04648.id;
SELECT 'LEFT remote', countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM jl_04648 LEFT JOIN jr_04648 ON jl_04648.id = jr_04648.id);

SELECT 'RIGHT', count() FROM jl_04648 RIGHT JOIN jr_04648 ON jl_04648.id = jr_04648.id;
SELECT 'RIGHT remote', countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM jl_04648 RIGHT JOIN jr_04648 ON jl_04648.id = jr_04648.id);

SELECT 'LEFT SEMI', count() FROM jl_04648 LEFT SEMI JOIN jr_04648 ON jl_04648.id = jr_04648.id;
SELECT 'LEFT SEMI remote', countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM jl_04648 LEFT SEMI JOIN jr_04648 ON jl_04648.id = jr_04648.id);

SELECT 'LEFT ANTI', count() FROM jl_04648 LEFT ANTI JOIN jr_04648 ON jl_04648.id = jr_04648.id;
SELECT 'LEFT ANTI remote', countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM jl_04648 LEFT ANTI JOIN jr_04648 ON jl_04648.id = jr_04648.id);

-- FULL must not be distributed and must not duplicate unmatched rows. Checked for both fragment paths.
SELECT 'FULL', count() FROM jl_04648 FULL JOIN jr_04648 ON jl_04648.id = jr_04648.id
SETTINGS parallel_replicas_local_plan = 1;
SELECT 'FULL lp=0', count() FROM jl_04648 FULL JOIN jr_04648 ON jl_04648.id = jr_04648.id
SETTINGS parallel_replicas_local_plan = 0;
SELECT 'FULL remote', countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM jl_04648 FULL JOIN jr_04648 ON jl_04648.id = jr_04648.id);

-- CROSS must not be distributed.
SELECT 'CROSS', count() FROM jl_04648 CROSS JOIN jr_04648;
SELECT 'CROSS remote', countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM jl_04648 CROSS JOIN jr_04648);

DROP TABLE jl_04648 SYNC;
DROP TABLE jr_04648 SYNC;
