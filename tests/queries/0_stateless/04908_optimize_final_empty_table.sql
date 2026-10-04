-- `OPTIMIZE FINAL` on an empty non-replicated table is a no-op. In particular,
-- it must not confuse a zero desired merge-executor reservation with shutdown.
DROP TABLE IF EXISTS t_optimize_final_empty;

CREATE TABLE t_optimize_final_empty (k UInt64)
ENGINE = MergeTree ORDER BY k;

OPTIMIZE TABLE t_optimize_final_empty FINAL;
OPTIMIZE TABLE t_optimize_final_empty FINAL
SETTINGS optimize_skip_merged_partitions = 1, optimize_throw_if_noop = 1;

DROP TABLE t_optimize_final_empty;
