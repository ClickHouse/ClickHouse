-- Tags: no-parallel, no-random-merge-tree-settings
-- Regression test: `MergeTreeDataPartWriterCompact::cancel` must not dereference
-- a null `shared_ptr` when `addStreams` fails before fully constructing the stream.

DROP TABLE IF EXISTS t_compact_cancel;

CREATE TABLE t_compact_cancel (a UInt64, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 10000000;

-- Prevent background merges from racing with the failpoint.
SYSTEM STOP MERGES t_compact_cancel;

INSERT INTO t_compact_cancel SELECT number, toString(number), number FROM numbers(100);
INSERT INTO t_compact_cancel SELECT number, toString(number), number FROM numbers(100, 100);

-- Force the Compact writer's `addStreams` to throw during OPTIMIZE (merge).
SYSTEM ENABLE FAILPOINT compact_part_writer_fail_in_add_streams;
SYSTEM START MERGES t_compact_cancel;

OPTIMIZE TABLE t_compact_cancel FINAL; -- {serverError FAULT_INJECTED}

SYSTEM DISABLE FAILPOINT compact_part_writer_fail_in_add_streams;

-- The server must still be alive and the table readable.
SELECT count() FROM t_compact_cancel;

DROP TABLE t_compact_cancel;
