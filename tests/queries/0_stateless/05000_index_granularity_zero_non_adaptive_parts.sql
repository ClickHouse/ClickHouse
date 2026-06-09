-- Tags: no-random-merge-tree-settings

-- `index_granularity = 0` makes granule sizing purely byte-driven. A non-adaptive part stores no per-mark
-- row count: its granularity is reconstructed from the table's `index_granularity` on load, which would
-- yield zero-row granules and unreadable data. Such parts must not be brought into a zero-granularity table.

DROP TABLE IF EXISTS t_non_adaptive_src;
DROP TABLE IF EXISTS t_zero_attach;
DROP TABLE IF EXISTS t_zero_replace;
DROP TABLE IF EXISTS t_zero_move;
DROP TABLE IF EXISTS t_adaptive_src;
DROP TABLE IF EXISTS t_zero_ok;

-- A source table with non-adaptive parts (index_granularity_bytes = 0).
CREATE TABLE t_non_adaptive_src (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_non_adaptive_src SELECT number, number FROM numbers(100000);

-- ATTACH / REPLACE / MOVE of a non-adaptive part into a zero-granularity table must all be rejected.
CREATE TABLE t_zero_attach (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 0, index_granularity_bytes = 10000000, min_bytes_for_wide_part = 0;
ALTER TABLE t_zero_attach ATTACH PARTITION ID 'all' FROM t_non_adaptive_src; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_zero_replace (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 0, index_granularity_bytes = 10000000, min_bytes_for_wide_part = 0;
ALTER TABLE t_zero_replace REPLACE PARTITION ID 'all' FROM t_non_adaptive_src; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_zero_move (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 0, index_granularity_bytes = 10000000, min_bytes_for_wide_part = 0;
ALTER TABLE t_non_adaptive_src MOVE PARTITION ID 'all' TO TABLE t_zero_move; -- { serverError BAD_ARGUMENTS }

-- Attaching an adaptive part into a zero-granularity table is fine: it stores its own per-mark row counts.
CREATE TABLE t_adaptive_src (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 8192, index_granularity_bytes = 10000000, min_bytes_for_wide_part = 0;
INSERT INTO t_adaptive_src SELECT number, number FROM numbers(100000);
CREATE TABLE t_zero_ok (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 0, index_granularity_bytes = 10000000, min_bytes_for_wide_part = 0;
ALTER TABLE t_zero_ok ATTACH PARTITION ID 'all' FROM t_adaptive_src;
SELECT count() FROM t_zero_ok;
SELECT sum(b) FROM t_zero_ok WHERE a = 0;

DROP TABLE t_non_adaptive_src;
DROP TABLE t_zero_attach;
DROP TABLE t_zero_replace;
DROP TABLE t_zero_move;
DROP TABLE t_adaptive_src;
DROP TABLE t_zero_ok;
