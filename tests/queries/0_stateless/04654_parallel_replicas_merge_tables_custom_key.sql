-- Parallel replicas over `Merge` tables and the `merge` table function are implemented only for the
-- read-tasks mode. In the custom-key modes such a query must fall back to single-replica execution
-- instead of addressing the remote read by the storage id, which for a table function does not exist
-- on the replicas.

DROP TABLE IF EXISTS t_pr_merge_ck_1;
DROP TABLE IF EXISTS t_pr_merge_ck_2;
DROP TABLE IF EXISTS t_pr_merge_ck;

CREATE TABLE t_pr_merge_ck_1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
CREATE TABLE t_pr_merge_ck_2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
INSERT INTO t_pr_merge_ck_1 SELECT number, number * 2 FROM numbers(1000);
INSERT INTO t_pr_merge_ck_2 SELECT number + 1000, number FROM numbers(1000);

CREATE TABLE t_pr_merge_ck ENGINE = Merge(currentDatabase(), '^t_pr_merge_ck_[12]$');

SELECT '-- non-parallel';
SELECT count(), sum(k), sum(v) FROM t_pr_merge_ck;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_allow_merge_tables = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_custom_key = 'k';

SET parallel_replicas_mode = 'custom_key_range';
SELECT '-- custom_key_range, Merge table';
SELECT count(), sum(k), sum(v) FROM t_pr_merge_ck;
SELECT '-- custom_key_range, merge() table function';
SELECT count(), sum(k), sum(v) FROM merge(currentDatabase(), '^t_pr_merge_ck_[12]$');

SET parallel_replicas_mode = 'custom_key_sampling';
SELECT '-- custom_key_sampling, Merge table';
SELECT count(), sum(k), sum(v) FROM t_pr_merge_ck;
SELECT '-- custom_key_sampling, merge() table function';
SELECT count(), sum(k), sum(v) FROM merge(currentDatabase(), '^t_pr_merge_ck_[12]$');

DROP TABLE t_pr_merge_ck;
DROP TABLE t_pr_merge_ck_2;
DROP TABLE t_pr_merge_ck_1;
