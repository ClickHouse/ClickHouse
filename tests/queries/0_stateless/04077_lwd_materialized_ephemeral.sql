-- Test: MATERIALIZED and DEFAULT (EPHEMERAL-style) columns are correct after LWD.

DROP TABLE IF EXISTS t_lwd_materialized;

CREATE TABLE t_lwd_materialized
(
    a     UInt32,
    b     UInt32,
    b2    UInt32 MATERIALIZED b * 2,
    label String DEFAULT 'ok'
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_lwd_materialized (a, b) SELECT number, number FROM numbers(100);

SET lightweight_deletes_sync = 2;
DELETE FROM t_lwd_materialized WHERE a < 20;

-- Count is correct after LWD
SELECT count() = 80 FROM t_lwd_materialized;

-- MATERIALIZED column returns correct values for all surviving rows
SELECT count() = 0 FROM t_lwd_materialized WHERE b2 != b * 2;

-- DEFAULT column is intact on surviving rows
SELECT count() = 0 FROM t_lwd_materialized WHERE label != 'ok';

-- Spot check a specific surviving row
SELECT a, b, b2, label FROM t_lwd_materialized WHERE a = 50;

-- After merge the computed values must still be correct
OPTIMIZE TABLE t_lwd_materialized FINAL;
SELECT count() = 0 FROM t_lwd_materialized WHERE b2 != b * 2;

DROP TABLE t_lwd_materialized;
