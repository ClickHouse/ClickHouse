-- A part can hold a column the table no longer knows under that name: a partition detached before a
-- `DROP COLUMN` and re-attached after it keeps the dropped column on disk. Such a column is not
-- carried over into a mutated part, so dropping or clearing the part's remaining table-known columns
-- leaves a part with no columns at all - a part that cannot be loaded, whose every read throws
-- `Could not find a column of minimum size`, and whose mutation throws `Cannot calculate columns
-- sizes when columns or checksums are not initialized`. The `ALTER` must be refused instead, the
-- same way it is refused when the dropped columns are all the part has.

DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_wide;

CREATE TABLE t_compact (c0 UInt64, c1 UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

INSERT INTO t_compact VALUES (1, 2);

ALTER TABLE t_compact DETACH PARTITION ID 'all';
ALTER TABLE t_compact ADD COLUMN c2 UInt64 DEFAULT 7;
ALTER TABLE t_compact DROP COLUMN c0;
ALTER TABLE t_compact ATTACH PARTITION ID 'all';

SELECT part_type, arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_compact' AND active GROUP BY part_type;

ALTER TABLE t_compact CLEAR COLUMN c1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_compact DROP COLUMN c1; -- { serverError BAD_ARGUMENTS }

SELECT c1, c2 FROM t_compact;

CREATE TABLE t_wide (c0 UInt64, c1 UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_wide VALUES (1, 2);

ALTER TABLE t_wide DETACH PARTITION ID 'all';
ALTER TABLE t_wide ADD COLUMN c2 UInt64 DEFAULT 7;
ALTER TABLE t_wide DROP COLUMN c0;
ALTER TABLE t_wide ATTACH PARTITION ID 'all';

SELECT part_type, arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_wide' AND active GROUP BY part_type;

ALTER TABLE t_wide CLEAR COLUMN c1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_wide DROP COLUMN c1; -- { serverError BAD_ARGUMENTS }

SELECT c1, c2 FROM t_wide;

DROP TABLE t_compact;
DROP TABLE t_wide;
