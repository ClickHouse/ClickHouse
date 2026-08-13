-- Tags: no-replicated-database
-- Tag no-replicated-database: the test asserts the APPEND/non-APPEND distinction, and a
-- Replicated database refuses to create the non-APPEND refreshable view over a plain
-- MergeTree target (StorageMaterializedView.cpp, BAD_ARGUMENTS), so that arm cannot exist.

-- Plan-based parallel replicas expands a MaterializedView into a plain read of its target table, so
-- eligibility is governed by the target storage: a MaterializedView over a ReplicatedMergeTree is
-- distributed, over a plain MergeTree it needs parallel_replicas_for_non_replicated_merge_tree.
-- A REFRESHABLE MV that swaps its target on each refresh (non-APPEND) is kept local: the read is
-- shipped by name and re-resolved per replica without RefreshTask's sync/lock, so a refresh could swap
-- the target from under the remote read. An APPEND refreshable MV reads a fixed target, like a regular
-- MV, and is distributed. See PR #111063 review.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
SET allow_experimental_refreshable_materialized_view = 1;

DROP TABLE IF EXISTS t_pr_mv_src SYNC;
DROP VIEW IF EXISTS mv_pr_repl SYNC;
DROP VIEW IF EXISTS mv_pr_plain SYNC;
DROP VIEW IF EXISTS mv_pr_append SYNC;
DROP VIEW IF EXISTS mv_pr_replace SYNC;

CREATE TABLE t_pr_mv_src (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_mv_src SELECT number FROM numbers(1000);

-- MaterializedView with a ReplicatedMergeTree target.
CREATE MATERIALIZED VIEW mv_pr_repl
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/mv_pr_repl', '1') ORDER BY a
AS SELECT a FROM t_pr_mv_src;
INSERT INTO t_pr_mv_src SELECT number FROM numbers(1000);

-- MaterializedView with a plain MergeTree target.
CREATE MATERIALIZED VIEW mv_pr_plain
ENGINE = MergeTree ORDER BY a
AS SELECT a FROM t_pr_mv_src;
INSERT INTO t_pr_mv_src SELECT number FROM numbers(1000);

-- APPEND refreshable MaterializedView (fixed target). EMPTY skips the initial refresh so one explicit
-- refresh yields a deterministic row count.
CREATE MATERIALIZED VIEW mv_pr_append
REFRESH EVERY 1 YEAR APPEND (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/mv_pr_append', '1') ORDER BY a
EMPTY
AS SELECT a FROM t_pr_mv_src;
SYSTEM REFRESH VIEW mv_pr_append;
SYSTEM WAIT VIEW mv_pr_append;

-- REPLACE refreshable MaterializedView (swaps its target on each refresh) over a plain MergeTree.
CREATE MATERIALIZED VIEW mv_pr_replace
REFRESH EVERY 1 YEAR
ENGINE = MergeTree ORDER BY a
AS SELECT a FROM t_pr_mv_src;
SYSTEM WAIT VIEW mv_pr_replace;

-- ReplicatedMergeTree target: correct result, distributed.
SELECT count() FROM mv_pr_repl;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM mv_pr_repl);

-- Plain MergeTree target: opt-in OFF -> not distributed; opt-in ON -> distributed. Correct result both.
SELECT count() FROM mv_pr_plain SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM mv_pr_plain SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0);
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM mv_pr_plain SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1);

-- APPEND refreshable MV (fixed ReplicatedMergeTree target): correct result, distributed.
SELECT count() FROM mv_pr_append;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM mv_pr_append);

-- REPLACE refreshable MV (swap target): correct result, NOT distributed even with the opt-in on.
SELECT count() FROM mv_pr_replace SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM mv_pr_replace SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1);

DROP VIEW mv_pr_replace SYNC;
DROP VIEW mv_pr_append SYNC;
DROP VIEW mv_pr_plain SYNC;
DROP VIEW mv_pr_repl SYNC;
DROP TABLE t_pr_mv_src SYNC;
