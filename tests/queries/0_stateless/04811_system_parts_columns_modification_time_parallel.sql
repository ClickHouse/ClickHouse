-- Checks the values of `column_modification_time` in `system.parts_columns`, which are filled by a
-- parallel pre-pass over the parts. Guards against entries being dropped or attributed to the wrong
-- part, including when parts of the same table have different column lists.

DROP TABLE IF EXISTS t_parts_columns_mtime_wide;
DROP TABLE IF EXISTS t_parts_columns_mtime_compact;
DROP TABLE IF EXISTS t_parts_columns_mtime_empty;

-- `PARTITION BY k` gives one part per row, so a single INSERT produces enough parts for the pre-pass
-- to dispatch several workers. One part per partition also means nothing can be merged away, so the
-- part count is deterministic.
-- `min_bytes_for_full_part_storage = 0` pins Full storage against the test harness randomizing it:
-- in Packed storage every column reports the part directory's mtime, so the per-column `stat` path
-- this test covers would not run at all.
CREATE TABLE t_parts_columns_mtime_wide (k UInt64, a UInt64, b UInt64, c String, d UInt64)
    ENGINE = MergeTree PARTITION BY k ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

CREATE TABLE t_parts_columns_mtime_compact (k UInt64, a UInt64, b UInt64, c String, d UInt64)
    ENGINE = MergeTree PARTITION BY k ORDER BY k
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

INSERT INTO t_parts_columns_mtime_wide SELECT number, number, number, toString(number), number FROM numbers(0, 40);

-- Separate the batches in time, and add a column between them so that the two groups of parts have
-- different column lists -- that is where positional indexing in the pre-pass can go wrong.
SELECT sleep(2) FORMAT Null;
ALTER TABLE t_parts_columns_mtime_wide ADD COLUMN e UInt64;

INSERT INTO t_parts_columns_mtime_wide SELECT number, number, number, toString(number), number, number FROM numbers(40, 40);
INSERT INTO t_parts_columns_mtime_compact SELECT number, number, number, toString(number), number FROM numbers(40);

-- Each part must have the expected format, otherwise the rest of the test proves nothing.
SELECT count() = 80, countIf(part_type = 'Wide') = 80
FROM system.parts WHERE database = currentDatabase() AND table = 't_parts_columns_mtime_wide' AND active;

SELECT count() = 40, countIf(part_type = 'Compact') = 40
FROM system.parts WHERE database = currentDatabase() AND table = 't_parts_columns_mtime_compact' AND active;

-- One row per (part, column), each with a non-null time, for both part formats.
-- 40 parts of 5 columns plus 40 parts of 6 columns.
SELECT count() = 440, countIf(column_modification_time IS NULL) = 0
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_columns_mtime_wide' AND active;

SELECT count() = 200, countIf(column_modification_time IS NULL) = 0
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_columns_mtime_compact' AND active;

-- Every part must report its own column positions exactly once, with no gaps: an entry dropped,
-- duplicated or shifted by the pre-pass would break this pairing.
SELECT count() = 80
FROM (
    SELECT name FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_parts_columns_mtime_wide' AND active
    GROUP BY name
    HAVING arraySort(groupArray(column_position)) = range(1, count() + 1)
       AND countIf(column_modification_time IS NOT NULL) = count()
);

-- The parts really do have two different column counts, so the check above is meaningful.
SELECT uniqExact(n) = 2, min(n) = 5, max(n) = 6
FROM (
    SELECT name, count() AS n FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_parts_columns_mtime_wide' AND active
    GROUP BY name
);

-- Every part's time must belong to the batch that actually wrote it. If the pre-pass attributed a
-- part's times to a different part, the two batches would overlap here.
SELECT maxIf(column_modification_time, toUInt64(partition) < 40) < minIf(column_modification_time, toUInt64(partition) >= 40)
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_columns_mtime_wide' AND active;

-- Times must be sane -- within the lifetime of the test, not an exact clock comparison.
SELECT countIf(column_modification_time < (now() - toIntervalDay(1)) OR column_modification_time > (now() + toIntervalMinute(10))) = 0
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_columns_mtime_wide' AND active;

-- The pre-pass is skipped entirely when `column_modification_time` is not selected. Read a different
-- per-column field to cover that path.
SELECT count() = 440, countIf(column_ttl_min IS NULL) = 440
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_columns_mtime_wide' AND active;

-- A table with no parts must not trip the pre-pass either.
CREATE TABLE t_parts_columns_mtime_empty (k UInt64) ENGINE = MergeTree ORDER BY k;
SELECT count() = 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_parts_columns_mtime_empty' AND column_modification_time IS NOT NULL;

DROP TABLE t_parts_columns_mtime_wide;
DROP TABLE t_parts_columns_mtime_compact;
DROP TABLE t_parts_columns_mtime_empty;
