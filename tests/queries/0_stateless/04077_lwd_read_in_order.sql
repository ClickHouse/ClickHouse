-- Test: read_in_order optimization correctly skips LWD-deleted rows.

DROP TABLE IF EXISTS t_lwd_read_order;

CREATE TABLE t_lwd_read_order (a UInt32, b String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_lwd_read_order SELECT number, toString(number) FROM numbers(100);
OPTIMIZE TABLE t_lwd_read_order FINAL;

SET lightweight_deletes_sync = 2;
-- Delete all even-numbered rows; only odd ones should remain
DELETE FROM t_lwd_read_order WHERE a % 2 = 0;

-- Ascending order: first 5 odd values
SELECT a FROM t_lwd_read_order ORDER BY a ASC LIMIT 5
SETTINGS optimize_read_in_order = 1, read_in_order_two_level_merge_threshold = 1;

-- Descending order: last 5 odd values
SELECT a FROM t_lwd_read_order ORDER BY a DESC LIMIT 5
SETTINGS optimize_read_in_order = 1, read_in_order_two_level_merge_threshold = 1;

-- Total count via read_in_order path
SELECT count() FROM t_lwd_read_order
SETTINGS optimize_read_in_order = 1;

DROP TABLE t_lwd_read_order;
