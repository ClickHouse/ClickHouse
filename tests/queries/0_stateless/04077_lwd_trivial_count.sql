-- Test: COUNT(*) and trivial LIMIT return correct results when LWD markers are present.
-- optimize_trivial_count_query uses part-level row counts, which include deleted rows.
-- The implementation must account for has_lightweight_delete when deciding to skip scan.

DROP TABLE IF EXISTS t_lwd_trivial;

CREATE TABLE t_lwd_trivial (a UInt32)
    ENGINE = MergeTree ORDER BY a;

INSERT INTO t_lwd_trivial SELECT number FROM numbers(1000);
OPTIMIZE TABLE t_lwd_trivial FINAL;

SET lightweight_deletes_sync = 2;
DELETE FROM t_lwd_trivial WHERE a < 100;

-- COUNT must return 900, not the stale part-level count of 1000
SELECT count() FROM t_lwd_trivial;

-- Same with the optimization explicitly enabled
SELECT count() FROM t_lwd_trivial SETTINGS optimize_trivial_count_query = 1;

-- LIMIT must not return more than the available non-deleted rows
SELECT count() FROM (SELECT a FROM t_lwd_trivial LIMIT 1000);

-- After merge: part row count matches actual count, optimization is safe again
OPTIMIZE TABLE t_lwd_trivial FINAL;
SELECT count() FROM t_lwd_trivial;

DROP TABLE t_lwd_trivial;
