-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: the branch below is reached only when the storage has no metadata version
--   to reason with, so an engine substituted for `MergeTree` keeps throwing the logical error that
--   the check above it deliberately throws.

-- A partition detached before `DROP COLUMN` and re-attached after it comes back with a part that
-- still carries the dropped column on disk. `MATERIALIZE TTL` on a table whose only TTL is a rows
-- TTL and that has `ttl_only_drop_parts` takes a fast path which marks every column it does not
-- rewrite as ignored, and an ignored column skipped the check for columns absent from the table:
-- the mutation read the column, its identifier did not resolve any more, and the mutation failed
-- with `UNKNOWN_IDENTIFIER` and kept being retried, wedging the table's mutation queue.

DROP TABLE IF EXISTS t_05210;
-- The part type is what decides which branch of the mutation command split runs, so pin it here
-- rather than leave it to the randomized `min_bytes_for_wide_part` of the test run.
CREATE TABLE t_05210 (id UInt64, val UInt64, p UInt8, ts DateTime)
ENGINE = MergeTree PARTITION BY p ORDER BY id TTL ts + INTERVAL 30 YEAR
SETTINGS ttl_only_drop_parts = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
ALTER TABLE t_05210 ADD COLUMN c UInt32;
INSERT INTO t_05210 SELECT number, number, 1, now(), 42 FROM numbers(100);

ALTER TABLE t_05210 DETACH PARTITION 1;
-- No attached part has the column, so the drop is metadata-only.
ALTER TABLE t_05210 DROP COLUMN c;
ALTER TABLE t_05210 ATTACH PARTITION 1;

SELECT 'the part type', any(part_type) FROM system.parts
WHERE database = currentDatabase() AND table = 't_05210' AND active;
SELECT 'the part still has the dropped column', countIf(column = 'c') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_05210' AND active;

ALTER TABLE t_05210 MATERIALIZE TTL SETTINGS mutations_sync = 2;

-- The TTL is 30 years out, so nothing expires and the rows have to survive the rewrite. `val` is
-- summed rather than counted: the rewritten part takes its columns from the interpreter header, so a
-- column left out of it would be read back as defaults, which a row count cannot tell apart.
SELECT 'rows after materializing the TTL', count(), sum(val) FROM t_05210;
SELECT 'the rewrite dropped the column', countIf(column = 'c') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_05210' AND active;
SELECT 'unfinished mutations', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_05210' AND NOT is_done;

DROP TABLE t_05210;
