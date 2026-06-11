-- Tags: no-replicated-database, no-shared-merge-tree

-- Regression test for issue #98585.
-- A lightweight DELETE on a table with non-adaptive index granularity
-- (`index_granularity_bytes = 0`) used to abort the server in debug builds with
-- `Logical error: Incorrect mark rows for part ..., in-memory N, on disk 8192`
-- after a DETACH/ATTACH cycle. The DETACH/ATTACH triggers
-- `MergeTreeIndexGranularityConstant::fixFromRowsCount`, which sets
-- `last_mark_granularity` to the actual row count. The validator in
-- `MergeTreeDataPartWriterWide::validateColumnOfFixedSize` then compared that
-- in-memory value to the writer-side fallback `fixed_index_granularity`, which
-- never matched for an incomplete last mark.

DROP TABLE IF EXISTS t_lwd_nonadaptive;

-- min_rows_for_wide_part / min_bytes_for_wide_part must be 0 because tables with
-- non-adaptive granularity can only store wide parts; otherwise the server emits
-- a startup warning that turns the test red on the CI test runner.
CREATE TABLE t_lwd_nonadaptive (c0 Int)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_lwd_nonadaptive VALUES (1);

DETACH TABLE t_lwd_nonadaptive SYNC;
ATTACH TABLE t_lwd_nonadaptive;

DELETE FROM t_lwd_nonadaptive WHERE TRUE SETTINGS lightweight_deletes_sync = 2;

SELECT count() FROM t_lwd_nonadaptive;

DROP TABLE t_lwd_nonadaptive;

-- Also exercise the case where the last mark is non-empty after a partial delete.
DROP TABLE IF EXISTS t_lwd_nonadaptive_partial;

CREATE TABLE t_lwd_nonadaptive_partial (c0 Int)
ENGINE = MergeTree() ORDER BY c0
SETTINGS index_granularity_bytes = 0, index_granularity = 32, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_lwd_nonadaptive_partial SELECT number FROM numbers(100);

DETACH TABLE t_lwd_nonadaptive_partial SYNC;
ATTACH TABLE t_lwd_nonadaptive_partial;

DELETE FROM t_lwd_nonadaptive_partial WHERE c0 < 50 SETTINGS lightweight_deletes_sync = 2;

SELECT count() FROM t_lwd_nonadaptive_partial;
SELECT count() FROM t_lwd_nonadaptive_partial WHERE c0 < 50;

DROP TABLE t_lwd_nonadaptive_partial;
