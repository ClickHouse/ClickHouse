-- Verify that querying column_modification_time from system.parts_columns returns correct,
-- fully-populated values. This column is computed for every (part, column) pair via a parallel
-- pre-pass over the parts; this test guards against the parallelism dropping, duplicating or
-- misindexing entries.

DROP TABLE IF EXISTS t_parts_columns_mtime;

CREATE TABLE t_parts_columns_mtime (k UInt64, a UInt64, b UInt64, c String)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0;

-- Several separate INSERTs => several wide parts. Each column is a separate file, so
-- column_modification_time is a real per-column value.
INSERT INTO t_parts_columns_mtime VALUES (1, 1, 1, 'x');
INSERT INTO t_parts_columns_mtime VALUES (2, 2, 2, 'y');
INSERT INTO t_parts_columns_mtime VALUES (3, 3, 3, 'z');
INSERT INTO t_parts_columns_mtime VALUES (4, 4, 4, 'w');

-- All parts must be Wide (so the per-column modification time is meaningful).
SELECT count() = countIf(part_type = 'Wide')
FROM system.parts
WHERE database = currentDatabase() AND table = 't_parts_columns_mtime' AND active;

-- One row per (active part, column). The table has 4 columns.
SELECT count(), count() = (SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_parts_columns_mtime' AND active) * 4
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_parts_columns_mtime' AND active;

-- Every column of every active Wide part must have a non-null modification time.
SELECT countIf(column_modification_time IS NULL)
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_parts_columns_mtime' AND active;

-- The reported time must be sane (within the lifetime of the test, not an exact clock comparison).
SELECT countIf(column_modification_time < (now() - toIntervalDay(1)) OR column_modification_time > (now() + toIntervalMinute(10)))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_parts_columns_mtime' AND active;

DROP TABLE t_parts_columns_mtime;
