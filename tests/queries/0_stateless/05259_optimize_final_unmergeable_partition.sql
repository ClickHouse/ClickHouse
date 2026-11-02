-- `OPTIMIZE FINAL` on a table with several partitions must report a partition it could not merge.

DROP TABLE IF EXISTS t_optimize_final_unmergeable_partition;

CREATE TABLE t_optimize_final_unmergeable_partition (p UInt8, k UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY k;

INSERT INTO t_optimize_final_unmergeable_partition VALUES (1, 1);
INSERT INTO t_optimize_final_unmergeable_partition VALUES (2, 1);

-- Parts written before and after ADD PROJECTION have different projection sets and are never merged together.
ALTER TABLE t_optimize_final_unmergeable_partition ADD PROJECTION prj (SELECT k ORDER BY k);
INSERT INTO t_optimize_final_unmergeable_partition VALUES (2, 2);

OPTIMIZE TABLE t_optimize_final_unmergeable_partition FINAL SETTINGS optimize_throw_if_noop = 1; -- { serverError CANNOT_ASSIGN_OPTIMIZE }

SELECT 'unmergeable', partition, count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_optimize_final_unmergeable_partition' AND active AND partition = '2'
GROUP BY partition ORDER BY partition;

DROP TABLE t_optimize_final_unmergeable_partition;

-- With merges stopped, the query must fail without merging any partition.
DROP TABLE IF EXISTS t_optimize_final_merges_stopped;

CREATE TABLE t_optimize_final_merges_stopped (p UInt8, k UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY k;

SYSTEM STOP MERGES t_optimize_final_merges_stopped;

INSERT INTO t_optimize_final_merges_stopped VALUES (1, 1);
INSERT INTO t_optimize_final_merges_stopped VALUES (1, 2);
INSERT INTO t_optimize_final_merges_stopped VALUES (2, 1);
INSERT INTO t_optimize_final_merges_stopped VALUES (2, 2);

OPTIMIZE TABLE t_optimize_final_merges_stopped FINAL; -- { serverError ABORTED }

SELECT 'merges stopped', partition, count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_optimize_final_merges_stopped' AND active
GROUP BY partition ORDER BY partition;

DROP TABLE t_optimize_final_merges_stopped;
