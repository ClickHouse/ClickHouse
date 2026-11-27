-- Test: has_lightweight_delete flag in system.parts transitions S0 -> S1 -> S2

DROP TABLE IF EXISTS t_lwd_flag;

CREATE TABLE t_lwd_flag (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_lwd_flag SELECT number, number FROM numbers(1000);
OPTIMIZE TABLE t_lwd_flag FINAL;

-- S0: no LWD — has_lightweight_delete must be 0 for all parts
SELECT 's0_no_flag:', count() = 0
FROM system.parts
WHERE table = 't_lwd_flag' AND database = currentDatabase() AND active AND has_lightweight_delete;

SET lightweight_deletes_sync = 2;
DELETE FROM t_lwd_flag WHERE a < 100;

-- S1: has_lightweight_delete must be 1 for at least one active part
SELECT 's1_has_flag:', count() > 0
FROM system.parts
WHERE table = 't_lwd_flag' AND database = currentDatabase() AND active AND has_lightweight_delete;

-- COUNT must return correct value — not cached pre-LWD metadata
SELECT 's1_count:', count() = 900 FROM t_lwd_flag;
SELECT 's1_deleted_gone:', count() = 0 FROM t_lwd_flag WHERE a < 100;

-- Merge away the LWD markers
OPTIMIZE TABLE t_lwd_flag FINAL;

-- S2: has_lightweight_delete must be 0 again after merge
SELECT 's2_no_flag:', count() = 0
FROM system.parts
WHERE table = 't_lwd_flag' AND database = currentDatabase() AND active AND has_lightweight_delete;

-- Results still correct after merge
SELECT 's2_count:', count() = 900 FROM t_lwd_flag;

DROP TABLE t_lwd_flag;
