-- `ReplacingMergeTree` with `FINAL` may be read in reverse order (`optimize_read_in_reverse_order_final`).
-- The sequential partition optimization must then consume the partitions in descending order of the
-- partition-related sorting key column, otherwise `ORDER BY key DESC LIMIT n` returns the tail of the
-- first partition instead of the global top-n.

DROP TABLE IF EXISTS t_seq_rev;

CREATE TABLE t_seq_rev (k UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k PARTITION BY intDiv(k, 25) SETTINGS index_granularity = 8;
INSERT INTO t_seq_rev SELECT number, number FROM numbers(100);
INSERT INTO t_seq_rev SELECT number, number * 10 FROM numbers(100);

SET do_not_merge_across_partitions_select_final = 1, optimize_read_in_order = 1, optimize_read_in_reverse_order_final = 1;
SET optimize_final_limit_pushdown = 1, optimize_final_sequential_partitions = 1, max_threads = 4;

SELECT 'sequential partitions are used for the reverse read';
SELECT countIf(explain LIKE '%Concat 4 → 1%'), countIf(explain LIKE '%InReverseOrder%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_seq_rev FINAL ORDER BY k DESC LIMIT 5);

SELECT 'desc, optimized';
SELECT k, v FROM t_seq_rev FINAL ORDER BY k DESC LIMIT 5;
SELECT 'desc, reference';
SELECT k, v FROM t_seq_rev FINAL ORDER BY k DESC LIMIT 5 SETTINGS optimize_final_limit_pushdown = 0, optimize_final_sequential_partitions = 0;
SELECT 'desc with offset, optimized';
SELECT k, v FROM t_seq_rev FINAL ORDER BY k DESC LIMIT 3 OFFSET 24;
SELECT 'desc with offset, reference';
SELECT k, v FROM t_seq_rev FINAL ORDER BY k DESC LIMIT 3 OFFSET 24 SETTINGS optimize_final_limit_pushdown = 0, optimize_final_sequential_partitions = 0;
SELECT 'asc, optimized';
SELECT k, v FROM t_seq_rev FINAL ORDER BY k LIMIT 5;

DROP TABLE t_seq_rev;
