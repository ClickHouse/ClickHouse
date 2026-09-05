-- Plan-based parallel replicas distributes the whole eligible JOIN: the split is lifted above the
-- (logical) join, so each replica joins its coordinated portion of one side against a full
-- (broadcast) read of the other side, and the per-replica results are unioned. The fragment ships the
-- logical join and each replica/the initiator converts it to physical and pushes down runtime filters.
-- Results must equal non-parallel execution for both fragment execution paths. See PR #111063 review.

DROP TABLE IF EXISTS jd_l SYNC;
DROP TABLE IF EXISTS jd_r SYNC;

CREATE TABLE jd_l (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE jd_r (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO jd_l SELECT number FROM numbers(200000);            -- 0 .. 199999
INSERT INTO jd_r SELECT number + 100000 FROM numbers(200000);   -- 100000 .. 299999 (overlap 100000..199999)

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- INNER: distributed. Correct count for both fragment paths (local_plan on/off) and with the failpoint
-- that slows the initiator's local read so remote replicas emit first.
SELECT 'INNER', count() FROM jd_l INNER JOIN jd_r ON jd_l.id = jd_r.id SETTINGS parallel_replicas_local_plan = 0;
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;
SELECT 'INNER', count() FROM jd_l INNER JOIN jd_r ON jd_l.id = jd_r.id SETTINGS parallel_replicas_local_plan = 1;
SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;
SELECT 'INNER remote', countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM jd_l INNER JOIN jd_r ON jd_l.id = jd_r.id);

-- LEFT / RIGHT: distributed, correct for both paths.
SELECT 'LEFT', count() FROM jd_l LEFT JOIN jd_r ON jd_l.id = jd_r.id SETTINGS parallel_replicas_local_plan = 0;
SELECT 'LEFT', count() FROM jd_l LEFT JOIN jd_r ON jd_l.id = jd_r.id SETTINGS parallel_replicas_local_plan = 1;
SELECT 'RIGHT', count() FROM jd_l RIGHT JOIN jd_r ON jd_l.id = jd_r.id SETTINGS parallel_replicas_local_plan = 0;
SELECT 'RIGHT', count() FROM jd_l RIGHT JOIN jd_r ON jd_l.id = jd_r.id SETTINGS parallel_replicas_local_plan = 1;

-- LEFT SEMI / ANTI: distributed, correct.
SELECT 'LEFT SEMI', count() FROM jd_l LEFT SEMI JOIN jd_r ON jd_l.id = jd_r.id SETTINGS parallel_replicas_local_plan = 0;
SELECT 'LEFT ANTI', count() FROM jd_l LEFT ANTI JOIN jd_r ON jd_l.id = jd_r.id SETTINGS parallel_replicas_local_plan = 0;

-- FULL is not distributed (kept local); correct result.
SELECT 'FULL', count() FROM jd_l FULL JOIN jd_r ON jd_l.id = jd_r.id;
SELECT 'FULL remote', countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM jd_l FULL JOIN jd_r ON jd_l.id = jd_r.id);

DROP TABLE jd_l SYNC;
DROP TABLE jd_r SYNC;
