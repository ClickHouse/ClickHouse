-- Regression test: plan-based parallel replicas with parallel_replicas_local_plan = 0 must
-- still distribute the read via a remote-only fragment over all replicas

DROP TABLE IF EXISTS t_pr_remote_only;
DROP TABLE IF EXISTS t_pr_remote_only_2;
DROP TABLE IF EXISTS t_pr_name_clash;

CREATE TABLE t_pr_remote_only (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_remote_only SELECT number, number % 10 FROM numbers(100000);

CREATE TABLE t_pr_remote_only_2 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_remote_only_2 SELECT number FROM numbers(1000);

CREATE TABLE t_pr_name_clash (a UInt64, __parallel_replicas_fragment_dummy String) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_name_clash SELECT number, 'v' FROM numbers(1000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
-- No local plan: exercise the remote-only branch of createParallelReplicasPlan.
SET parallel_replicas_local_plan = 0;
-- The empty fragment header only arises when this pass runs, and the test runner randomizes the
-- setting off, where the cases below would pass without reaching the shape they are about.
SET query_plan_remove_unused_columns = 1;

SELECT count(), sum(b), min(a), max(a) FROM t_pr_remote_only WHERE a > 5;

-- Plan shape. Before optimization the planner produces a plain local plan with no split marker
-- After optimization the parallel-replicas analysis inserts the split above the read and
-- replaces it with a remote parallel-replicas read of the shipped fragment, with no local read and no
-- local/remote union
SELECT
    countIf(explain LIKE '%ParallelReplicasSplit%') > 0 AS has_split,
    countIf(explain LIKE '%Union%') > 0 AS has_union,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS has_read,
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 0, description = 0 SELECT sum(b) FROM t_pr_remote_only WHERE a > 5);

SELECT
    countIf(explain LIKE '%ParallelReplicasSplit%') > 0 AS has_split,
    countIf(explain LIKE '%Union%') > 0 AS has_union,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS has_read,
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0 SELECT sum(b) FROM t_pr_remote_only WHERE a > 5);

-- A LIMIT below the aggregate keeps the aggregate out of the distributed fragment, so nothing in the
-- fragment needs a column of its own and its output header is pruned empty. Every row must still be
-- counted. Each query is followed by the same query without parallel replicas, which is the expected value.
SELECT count() FROM (SELECT 1 FROM t_pr_remote_only LIMIT 500);
SELECT count() FROM (SELECT 1 FROM t_pr_remote_only LIMIT 500) SETTINGS enable_parallel_replicas = 0;

-- Local execution produces those same counts, so pin the shape of that query as well: it is
-- distributed, and the header of the shipped fragment is not empty.
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%__parallel_replicas_fragment_dummy%') > 0 AS has_fragment_placeholder
FROM (EXPLAIN header = 1, optimize = 1, description = 0 SELECT count() FROM (SELECT 1 FROM t_pr_remote_only LIMIT 500));

SELECT count() FROM (SELECT 'x' FROM t_pr_remote_only LIMIT 300);
SELECT count() FROM (SELECT 'x' FROM t_pr_remote_only LIMIT 300) SETTINGS enable_parallel_replicas = 0;

SELECT count() FROM (SELECT 1 FROM t_pr_remote_only LIMIT 200 OFFSET 100);
SELECT count() FROM (SELECT 1 FROM t_pr_remote_only LIMIT 200 OFFSET 100) SETTINGS enable_parallel_replicas = 0;

-- Not specific to count(): any aggregate over a constant projection prunes the same way.
SELECT sum(c) FROM (SELECT 1 AS c FROM t_pr_remote_only LIMIT 400);
SELECT sum(c) FROM (SELECT 1 AS c FROM t_pr_remote_only LIMIT 400) SETTINGS enable_parallel_replicas = 0;

-- Without an aggregate the rows are returned directly, so the whole result set went missing.
SELECT c FROM (SELECT 1 AS c FROM t_pr_remote_only LIMIT 500) LIMIT 1;
SELECT c FROM (SELECT 1 AS c FROM t_pr_remote_only LIMIT 500) LIMIT 1 SETTINGS enable_parallel_replicas = 0;

-- One LIMITed branch of a UNION ALL prunes to an empty header while the other keeps its column.
SELECT count() FROM (SELECT a FROM t_pr_remote_only LIMIT 10 UNION ALL SELECT a FROM t_pr_remote_only_2);
SELECT count() FROM (SELECT a FROM t_pr_remote_only LIMIT 10 UNION ALL SELECT a FROM t_pr_remote_only_2) SETTINGS enable_parallel_replicas = 0;

SELECT src, count() FROM (SELECT 'A' src, a FROM t_pr_remote_only LIMIT 3
                          UNION ALL SELECT 'B' src, a FROM t_pr_remote_only_2 LIMIT 4) GROUP BY src ORDER BY src;
SELECT src, count() FROM (SELECT 'A' src, a FROM t_pr_remote_only LIMIT 3
                          UNION ALL SELECT 'B' src, a FROM t_pr_remote_only_2 LIMIT 4) GROUP BY src ORDER BY src
SETTINGS enable_parallel_replicas = 0;

-- The pruning is what empties the header, so keeping the pruned column also keeps the count right.
SELECT count() FROM (SELECT 1 FROM t_pr_remote_only LIMIT 500) SETTINGS query_plan_remove_unused_columns = 0;

-- A fragment that keeps a column of its own is unaffected.
SELECT count() FROM (SELECT a FROM t_pr_remote_only LIMIT 500);
SELECT count() FROM (SELECT a, 1 AS c FROM t_pr_remote_only LIMIT 500);
SELECT count() FROM (SELECT 1 FROM t_pr_remote_only WHERE a < 500);
SELECT count() FROM (SELECT 1 AS c FROM t_pr_remote_only ORDER BY c LIMIT 500);
SELECT count() FROM (SELECT DISTINCT 1 FROM t_pr_remote_only LIMIT 500);
SELECT count() FROM t_pr_remote_only;

-- A user column of the placeholder's name is only present when the header is not empty, so the
-- placeholder is never added alongside it.
SELECT count(), any(__parallel_replicas_fragment_dummy) FROM (SELECT __parallel_replicas_fragment_dummy FROM t_pr_name_clash LIMIT 500);
SELECT count() FROM (SELECT 1 FROM t_pr_name_clash LIMIT 500);

-- The local half of the local+remote union produces the same header as the remote half, so the
-- default parallel_replicas_local_plan = 1 is affected too whenever the remote half wins the race.
-- The failpoint slows the local read down to make that deterministic.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;
SELECT count() FROM (SELECT 1 FROM t_pr_remote_only LIMIT 500) SETTINGS parallel_replicas_local_plan = 1;
SELECT count() FROM (SELECT a FROM t_pr_remote_only LIMIT 500) SETTINGS parallel_replicas_local_plan = 1;
SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

DROP TABLE t_pr_remote_only;
DROP TABLE t_pr_remote_only_2;
DROP TABLE t_pr_name_clash;
