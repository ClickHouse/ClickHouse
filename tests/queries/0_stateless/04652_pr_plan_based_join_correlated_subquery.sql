-- Plan-based parallel replicas ships JoinStepLogical fragments to remote replicas, where they are
-- re-optimized from scratch (like make_distributed_plan). A correlated-subquery decorrelation join buffers
-- a common subplan through a shared in-memory ChunkBuffer whose reader/writer sit in opposite join sides;
-- those buffer steps are non-serializable/non-cloneable, so such a join must never enter a shipped
-- fragment. Verify correct results (no "Cannot clone ..." / "Trying to extract chunk from ChunkBuffer ..."
-- error) for both fragment execution paths. See PR #112268 review (comment r3665719117).

DROP TABLE IF EXISTS cs_l SYNC;
DROP TABLE IF EXISTS cs_r SYNC;

CREATE TABLE cs_l (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE cs_r (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO cs_l SELECT number, number FROM numbers(1000);
INSERT INTO cs_r SELECT number, number * 2 FROM numbers(1000);   -- one match per k, v = k * 2

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- Simple: the correlated subquery's buffer writer is on the coordinated side, so the split cannot lift
-- above it and the decorrelation join stays local. sum(s) = sum(k*2 for k in 0..999) = 999000.
SELECT 'CORR local_plan=0', sum(s) FROM (
    SELECT k, (SELECT sum(v) FROM cs_r WHERE cs_r.k = cs_l.k) AS s FROM cs_l
) SETTINGS parallel_replicas_local_plan = 0;

SELECT 'CORR local_plan=1', sum(s) FROM (
    SELECT k, (SELECT sum(v) FROM cs_r WHERE cs_r.k = cs_l.k) AS s FROM cs_l
) SETTINGS parallel_replicas_local_plan = 1;

-- Nested: a correlated subquery on the broadcast (right) side of an eligible LEFT join over a clean fact
-- read. The in-memory buffer steps are pinned present here; the split on the fact read would otherwise be
-- lifted above the outer join and pull the non-serializable buffer subtree into the shipped fragment. The
-- decorrelation join must stay local while the fact read still distributes. count() = 1000 (all fact rows).
DROP TABLE IF EXISTS nf SYNC;
DROP TABLE IF EXISTS nd SYNC;
DROP TABLE IF EXISTS nr SYNC;
CREATE TABLE nf (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE nd (k UInt64, y UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE nr (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO nf SELECT number, number FROM numbers(1000);
INSERT INTO nd SELECT number, number FROM numbers(1000);
INSERT INTO nr SELECT number, number * 2 FROM numbers(1000);

SET correlated_subqueries_use_in_memory_buffer = 1;
SET correlated_subqueries_substitute_equivalent_expressions = 0;

SELECT 'NESTED local_plan=0', count() FROM nf LEFT JOIN (
    SELECT k, (SELECT sum(v) FROM nr WHERE nr.k = nd.k) AS s FROM nd
) sub ON nf.k = sub.k SETTINGS parallel_replicas_local_plan = 0;

SELECT 'NESTED local_plan=1', count() FROM nf LEFT JOIN (
    SELECT k, (SELECT sum(v) FROM nr WHERE nr.k = nd.k) AS s FROM nd
) sub ON nf.k = sub.k SETTINGS parallel_replicas_local_plan = 1;

DROP TABLE cs_l SYNC;
DROP TABLE cs_r SYNC;
DROP TABLE nf SYNC;
DROP TABLE nd SYNC;
DROP TABLE nr SYNC;
