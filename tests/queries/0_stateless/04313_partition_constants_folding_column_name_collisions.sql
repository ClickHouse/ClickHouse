-- Tags: no-parallel-replicas

-- A physical column whose name collides with a partition virtual column (_partition_id /
-- _partition_value) or with a derived partition-key expression (toYYYYMM(d)) must not be confused
-- with the partition constant during folding. Results must match the unfolded path.

SET enable_analyzer = 1;

-- _partition_id physical column that is also the partition key
DROP TABLE IF EXISTS t_collision_pid;
CREATE TABLE t_collision_pid (`_partition_id` String, b UInt64)
ENGINE = MergeTree ORDER BY b PARTITION BY `_partition_id` SETTINGS index_granularity = 1;
INSERT INTO t_collision_pid VALUES ('a/b', 10), ('plain', 20);

SELECT '-- _partition_id --';
SELECT `_partition_id`, b FROM t_collision_pid WHERE `_partition_id` = 'a/b' AND b = 10 ORDER BY b
SETTINGS use_constant_folding_in_index_analysis = 0;
SELECT `_partition_id`, b FROM t_collision_pid WHERE `_partition_id` = 'a/b' AND b = 10 ORDER BY b
SETTINGS use_constant_folding_in_index_analysis = 1;
DROP TABLE t_collision_pid;

-- _partition_value physical column that is also the partition key
DROP TABLE IF EXISTS t_collision_pval;
CREATE TABLE t_collision_pval (`_partition_value` String, b UInt64)
ENGINE = MergeTree ORDER BY b PARTITION BY `_partition_value` SETTINGS index_granularity = 1;
INSERT INTO t_collision_pval VALUES ('a', 10), ('b', 20);

SELECT '-- _partition_value --';
SELECT `_partition_value`, b FROM t_collision_pval WHERE `_partition_value` = 'a' AND b = 10 ORDER BY b
SETTINGS use_constant_folding_in_index_analysis = 0;
SELECT `_partition_value`, b FROM t_collision_pval WHERE `_partition_value` = 'a' AND b = 10 ORDER BY b
SETTINGS use_constant_folding_in_index_analysis = 1;
DROP TABLE t_collision_pval;

-- physical column named like a derived partition-key expression
DROP TABLE IF EXISTS t_collision_expr;
CREATE TABLE t_collision_expr (d Date, `toYYYYMM(d)` UInt32, b UInt64)
ENGINE = MergeTree ORDER BY b PARTITION BY toYYYYMM(d) SETTINGS index_granularity = 1;
INSERT INTO t_collision_expr VALUES ('2024-01-01', 7, 10);

SELECT '-- toYYYYMM(d) --';
SELECT d, `toYYYYMM(d)`, b FROM t_collision_expr WHERE `toYYYYMM(d)` = 7 AND b = 10 ORDER BY b
SETTINGS use_partition_pruning = 0, use_skip_indexes = 0, use_constant_folding_in_index_analysis = 0;
SELECT d, `toYYYYMM(d)`, b FROM t_collision_expr WHERE `toYYYYMM(d)` = 7 AND b = 10 ORDER BY b
SETTINGS use_partition_pruning = 0, use_skip_indexes = 0, use_constant_folding_in_index_analysis = 1;
DROP TABLE t_collision_expr;
