-- Test: multiple delete-then-merge cycles; flag and correctness at each stage.

DROP TABLE IF EXISTS t_lwd_cycles;

CREATE TABLE t_lwd_cycles (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_lwd_cycles SELECT number, number FROM numbers(1000);

SET lightweight_deletes_sync = 2;

-- Cycle 1: delete a < 100
DELETE FROM t_lwd_cycles WHERE a < 100;
SELECT count() = 900 FROM t_lwd_cycles;

-- Merge (S1 -> S2: rows physically removed)
OPTIMIZE TABLE t_lwd_cycles FINAL;
SELECT count() = 900 FROM t_lwd_cycles;

-- After merge: flag cleared
SELECT count() = 0
FROM system.parts
WHERE table = 't_lwd_cycles' AND database = currentDatabase() AND active AND has_lightweight_delete;

-- Cycle 2: delete another range
DELETE FROM t_lwd_cycles WHERE a BETWEEN 100 AND 199;
SELECT count() = 800 FROM t_lwd_cycles;

-- Cycle 3: second LWD while cycle 2 mutation is still active (S1 + S1)
DELETE FROM t_lwd_cycles WHERE a BETWEEN 200 AND 299;
SELECT count() = 700 FROM t_lwd_cycles;

-- Final merge to physical cleanup
OPTIMIZE TABLE t_lwd_cycles FINAL;
SELECT count() = 700 FROM t_lwd_cycles;

-- All deleted ranges are truly gone
SELECT count() = 0 FROM t_lwd_cycles WHERE a < 300;

-- Remaining rows are intact
SELECT count() = 0 FROM t_lwd_cycles WHERE b != a;

DROP TABLE t_lwd_cycles;
