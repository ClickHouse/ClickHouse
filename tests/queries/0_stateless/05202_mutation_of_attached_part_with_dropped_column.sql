-- A partition detached before `DROP COLUMN` and re-attached after it comes back with a part that still
-- carries the dropped column on disk. Reads and merges ignore such a column, but a mutation of a
-- Compact part read it as a `READ_COLUMN` command, whose identifier does not resolve against the table
-- any more: the mutation failed with `UNKNOWN_IDENTIFIER` and, because the part is what is poisoned,
-- every later mutation failed the same way, wedging the table's mutation queue. Only a merge or
-- dropping the partition recovered.

DROP TABLE IF EXISTS t_05202;
CREATE TABLE t_05202 (id UInt64, val UInt64, p UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY id;
ALTER TABLE t_05202 ADD COLUMN c UInt32;
INSERT INTO t_05202 SELECT number, number, 1, 42 FROM numbers(100);

ALTER TABLE t_05202 DETACH PARTITION 1;
-- No attached part has the column, so the drop is metadata-only.
ALTER TABLE t_05202 DROP COLUMN c;
ALTER TABLE t_05202 ATTACH PARTITION 1;

SELECT 'the part type', any(part_type) FROM system.parts
WHERE database = currentDatabase() AND table = 't_05202' AND active;
SELECT 'the part still has the dropped column', countIf(column = 'c') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_05202' AND active;
SELECT 'reads are fine', count() FROM t_05202;

ALTER TABLE t_05202 DELETE WHERE id = 5 SETTINGS mutations_sync = 2;

SELECT 'rows after the delete', count() FROM t_05202;
SELECT 'the rewrite dropped the column', countIf(column = 'c') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_05202' AND active;
SELECT 'unfinished mutations', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_05202' AND NOT is_done;
CHECK TABLE t_05202 SETTINGS check_query_single_value_result = 1;

-- A second mutation still works: the part is no longer poisoned.
ALTER TABLE t_05202 UPDATE val = val + 1 WHERE id = 7 SETTINGS mutations_sync = 2;
SELECT 'after a second mutation', sum(val) FROM t_05202;

DROP TABLE t_05202;

-- The same with a Wide part, which was never affected.
DROP TABLE IF EXISTS t_05202_wide;
CREATE TABLE t_05202_wide (id UInt64, val UInt64, p UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
ALTER TABLE t_05202_wide ADD COLUMN c UInt32;
INSERT INTO t_05202_wide SELECT number, number, 1, 42 FROM numbers(100);

ALTER TABLE t_05202_wide DETACH PARTITION 1;
ALTER TABLE t_05202_wide DROP COLUMN c;
ALTER TABLE t_05202_wide ATTACH PARTITION 1;

SELECT 'the wide part type', any(part_type) FROM system.parts
WHERE database = currentDatabase() AND table = 't_05202_wide' AND active;

ALTER TABLE t_05202_wide DELETE WHERE id = 5 SETTINGS mutations_sync = 2;
SELECT 'rows after the delete', count() FROM t_05202_wide;
SELECT 'the rewrite dropped the column', countIf(column = 'c') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_05202_wide' AND active;

DROP TABLE t_05202_wide;
