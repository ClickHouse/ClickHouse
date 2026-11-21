-- A bucketed distributed FINAL read (`make_distributed_plan`) rebuilds the FINAL merge per lane on the
-- worker: it merges forward and does not receive `final_limit`. So (1) a reverse read direction must not
-- be announced for it, and (2) `optimize_final_limit_pushdown` / `optimize_final_sequential_partitions`
-- do not apply there. Both must leave the results identical to the plain non-distributed read.

DROP TABLE IF EXISTS t_dist_final_replacing;
DROP TABLE IF EXISTS t_dist_final_summing;

-- Distributed aggregation cannot enforce a global max_rows_to_group_by, so pin it to 0 (randomized
-- settings set it nonzero, which would make make_distributed_plan reject the aggregates below).
SET max_rows_to_group_by = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;
SET distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 0;
SET optimize_read_in_reverse_order_final = 1;
SET do_not_merge_across_partitions_select_final = 1;

-- Two fully overlapping parts with different versions; small granules so a LIMIT stops inside a part.
CREATE TABLE t_dist_final_replacing (k UInt64, version UInt64, v String) ENGINE = ReplacingMergeTree(version) ORDER BY k
SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_dist_final_replacing;
INSERT INTO t_dist_final_replacing SELECT number, 1, 'old' FROM numbers(30000);
INSERT INTO t_dist_final_replacing SELECT number, 2, 'new' FROM numbers(30000);

SELECT '-- Replacing, reverse read-in-order, local';
SELECT k, version, v FROM t_dist_final_replacing FINAL ORDER BY k DESC LIMIT 5 SETTINGS make_distributed_plan = 0;
SELECT sum(k), sum(version), count() FROM (SELECT k, version FROM t_dist_final_replacing FINAL ORDER BY k DESC LIMIT 1000) SETTINGS make_distributed_plan = 0;

SELECT '-- Replacing, reverse read-in-order, distributed';
SELECT k, version, v FROM t_dist_final_replacing FINAL ORDER BY k DESC LIMIT 5
SETTINGS make_distributed_plan = 1, distributed_plan_read_in_order = 1;
SELECT sum(k), sum(version), count() FROM (SELECT k, version FROM t_dist_final_replacing FINAL ORDER BY k DESC LIMIT 1000)
SETTINGS make_distributed_plan = 1, distributed_plan_read_in_order = 1;
SELECT k, version, v FROM t_dist_final_replacing FINAL ORDER BY k DESC LIMIT 5 OFFSET 12345
SETTINGS make_distributed_plan = 1, distributed_plan_read_in_order = 1;

-- Partitioned Summing table, so the sequential-partition path would be eligible on a direct read.
CREATE TABLE t_dist_final_summing (k UInt64, s UInt64) ENGINE = SummingMergeTree ORDER BY k PARTITION BY intDiv(k, 10000)
SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_dist_final_summing;
INSERT INTO t_dist_final_summing SELECT number, 1 FROM numbers(30000);
INSERT INTO t_dist_final_summing SELECT number, 10 FROM numbers(30000);

SELECT '-- Summing, limit pushdown settings, local';
SELECT k, s FROM t_dist_final_summing FINAL ORDER BY k LIMIT 5
SETTINGS make_distributed_plan = 0, optimize_final_limit_pushdown = 1, optimize_final_sequential_partitions = 1;
SELECT sum(k), sum(s), count() FROM (SELECT k, s FROM t_dist_final_summing FINAL ORDER BY k LIMIT 15000)
SETTINGS make_distributed_plan = 0, optimize_final_limit_pushdown = 1, optimize_final_sequential_partitions = 1;

SELECT '-- Summing, limit pushdown settings, distributed';
SELECT k, s FROM t_dist_final_summing FINAL ORDER BY k LIMIT 5
SETTINGS make_distributed_plan = 1, distributed_plan_read_in_order = 1, optimize_final_limit_pushdown = 1, optimize_final_sequential_partitions = 1;
SELECT sum(k), sum(s), count() FROM (SELECT k, s FROM t_dist_final_summing FINAL ORDER BY k LIMIT 15000)
SETTINGS make_distributed_plan = 1, distributed_plan_read_in_order = 1, optimize_final_limit_pushdown = 1, optimize_final_sequential_partitions = 1;
SELECT k, s FROM t_dist_final_summing FINAL ORDER BY k LIMIT 5 OFFSET 12345
SETTINGS make_distributed_plan = 1, distributed_plan_read_in_order = 1, optimize_final_limit_pushdown = 1, optimize_final_sequential_partitions = 1;

DROP TABLE t_dist_final_replacing;
DROP TABLE t_dist_final_summing;
