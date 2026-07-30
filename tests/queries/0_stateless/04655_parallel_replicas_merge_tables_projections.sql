-- The child plans of a `Merge` table are built and optimized while the pipeline is initialized, with a
-- context derived from the initiator's local plan, where parallel replicas are switched off. The child
-- reading steps, however, are wrapped into parallel replicas reading steps, so without restoring the
-- "initiator with projection support" flag the projection optimizations would take the initiator-local
-- child for a remote replica and replace the projection reads with an empty source, losing rows.

DROP TABLE IF EXISTS t_pr_merge_proj_1;
DROP TABLE IF EXISTS t_pr_merge_proj_2;
DROP TABLE IF EXISTS t_pr_merge_proj;

CREATE TABLE t_pr_merge_proj_1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
CREATE TABLE t_pr_merge_proj_2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;

-- Parts written before the projection is added do not have it, so a projection read leaves parent parts
-- to read from the table itself - the shape in which the initiator has to read the projection locally.
INSERT INTO t_pr_merge_proj_1 SELECT number, number * 2 FROM numbers(1000);
INSERT INTO t_pr_merge_proj_2 SELECT number + 1000, number FROM numbers(1000);

ALTER TABLE t_pr_merge_proj_1 ADD PROJECTION p (SELECT k, v ORDER BY v);
ALTER TABLE t_pr_merge_proj_2 ADD PROJECTION p (SELECT k, v ORDER BY v);

INSERT INTO t_pr_merge_proj_1 SELECT number + 2000, number FROM numbers(1000);
INSERT INTO t_pr_merge_proj_2 SELECT number + 3000, number FROM numbers(1000);

CREATE TABLE t_pr_merge_proj ENGINE = Merge(currentDatabase(), '^t_pr_merge_proj_[12]$');

SELECT '-- non-parallel';
SELECT count() FROM t_pr_merge_proj;
SELECT count(), sum(k) FROM t_pr_merge_proj WHERE v = 42;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_support_projection = 1;
SET parallel_replicas_allow_merge_tables = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_mark_segment_size = 10;
SET use_query_condition_cache = 0;
SET parallel_replicas_connect_timeout_ms = 30000;

-- Slow the initiator's local reads down so that the remote replicas actually get read tasks.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

SELECT '-- count() over a Merge table';
SELECT count() FROM t_pr_merge_proj;
SELECT '-- count() over the merge() table function';
SELECT count() FROM merge(currentDatabase(), '^t_pr_merge_proj_[12]$');

SELECT '-- projection read over a Merge table';
SELECT count(), sum(k) FROM t_pr_merge_proj WHERE v = 42;
SELECT '-- projection read over the merge() table function';
SELECT count(), sum(k) FROM merge(currentDatabase(), '^t_pr_merge_proj_[12]$') WHERE v = 42;

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

DROP TABLE t_pr_merge_proj;
DROP TABLE t_pr_merge_proj_2;
DROP TABLE t_pr_merge_proj_1;
