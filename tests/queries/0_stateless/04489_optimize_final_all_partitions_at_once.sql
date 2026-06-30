-- OPTIMIZE TABLE ... FINAL on a non-replicated table assigns and runs the merges of all
-- partitions at once instead of one partition at a time (issue #46770). This test checks the
-- correctness of the result: every partition must be merged into a single part.

DROP TABLE IF EXISTS t_optimize_final_all_parts;

CREATE TABLE t_optimize_final_all_parts (k UInt32, v UInt32)
ENGINE = MergeTree PARTITION BY k % 16 ORDER BY k
SETTINGS optimize_on_insert = 0;

-- Several inserts, so every partition ends up with more than one part.
INSERT INTO t_optimize_final_all_parts SELECT number, number FROM numbers(1000);
INSERT INTO t_optimize_final_all_parts SELECT number, number + 1 FROM numbers(1000);
INSERT INTO t_optimize_final_all_parts SELECT number, number + 2 FROM numbers(1000);

OPTIMIZE TABLE t_optimize_final_all_parts FINAL;

-- Every one of the 16 partitions must be merged into exactly one part.
SELECT countDistinct(partition) AS partitions, max(parts_per_partition) AS max_parts_per_partition
FROM
(
    SELECT partition, count() AS parts_per_partition
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_optimize_final_all_parts' AND active
    GROUP BY partition
);

SELECT 'total_active_parts', count()
FROM system.parts
WHERE database = currentDatabase() AND table = 't_optimize_final_all_parts' AND active;

SELECT 'rows', count() FROM t_optimize_final_all_parts;

-- Re-running with optimize_skip_merged_partitions must be a no-op and must not throw, even with
-- optimize_throw_if_noop, because every partition already consists of a single merged part.
OPTIMIZE TABLE t_optimize_final_all_parts FINAL
SETTINGS optimize_skip_merged_partitions = 1, optimize_throw_if_noop = 1;

SELECT 'after_skip_total_active_parts', count()
FROM system.parts
WHERE database = currentDatabase() AND table = 't_optimize_final_all_parts' AND active;

DROP TABLE t_optimize_final_all_parts;

-- A single partition exercises the sequential path; it must still work.
DROP TABLE IF EXISTS t_optimize_final_one_part;

CREATE TABLE t_optimize_final_one_part (k UInt32, v UInt32)
ENGINE = MergeTree ORDER BY k
SETTINGS optimize_on_insert = 0;

INSERT INTO t_optimize_final_one_part SELECT number, number FROM numbers(1000);
INSERT INTO t_optimize_final_one_part SELECT number, number FROM numbers(1000);

OPTIMIZE TABLE t_optimize_final_one_part FINAL;

SELECT 'one_partition_total_active_parts', count()
FROM system.parts
WHERE database = currentDatabase() AND table = 't_optimize_final_one_part' AND active;

DROP TABLE t_optimize_final_one_part;
