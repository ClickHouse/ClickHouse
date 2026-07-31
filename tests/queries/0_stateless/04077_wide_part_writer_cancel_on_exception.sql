-- Tags: no-parallel, no-random-merge-tree-settings
-- Regression test: MergeTreeDataPartWriterWide::cancel must not SIGSEGV
-- when addStreams fails mid-way leaving no null entries in column_streams.

DROP TABLE IF EXISTS t_wide_cancel;

CREATE TABLE t_wide_cancel (a UInt64, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- Prevent background merges from racing with the failpoint.
SYSTEM STOP MERGES t_wide_cancel;

INSERT INTO t_wide_cancel SELECT number, toString(number), number FROM numbers(100);
INSERT INTO t_wide_cancel SELECT number, toString(number), number FROM numbers(100, 100);

-- Force the Wide writer's addStreams to throw during OPTIMIZE (merge).
SYSTEM ENABLE FAILPOINT wide_part_writer_fail_in_add_streams;
SYSTEM START MERGES t_wide_cancel;

OPTIMIZE TABLE t_wide_cancel FINAL; -- {serverError FAULT_INJECTED}

SYSTEM DISABLE FAILPOINT wide_part_writer_fail_in_add_streams;

-- The server must still be alive and the table readable.
SELECT count() FROM t_wide_cancel;

DROP TABLE t_wide_cancel;
