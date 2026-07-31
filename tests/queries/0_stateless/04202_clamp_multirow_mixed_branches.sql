-- Test: exercises `clamp` per-row branch selection in a multi-row block where rows hit all
-- three branches (return min / return value / return max). The PR's only multi-row test
-- (`numbers(3)`) keeps every row inside the [min, max] range, so it never selects
-- `best_arg = 1` or `best_arg = 2` in the non-constant column path.
-- Covers: src/Functions/clamp.cpp:50-54 — `best_arg` reset and selection inside the per-row loop
DROP TABLE IF EXISTS t_clamp_mixed;
CREATE TABLE t_clamp_mixed (id UInt8, v Int64, lo Int64, hi Int64) ENGINE = Memory;
INSERT INTO t_clamp_mixed VALUES (1, 5, 10, 20)(2, 15, 10, 20)(3, 25, 10, 20);
-- Row 1: v < lo  -> returns lo (best_arg = 1)
-- Row 2: lo <= v <= hi -> returns v (best_arg = 0)
-- Row 3: v > hi  -> returns hi (best_arg = 2)
SELECT id, clamp(v, lo, hi) FROM t_clamp_mixed ORDER BY id;
DROP TABLE t_clamp_mixed;
