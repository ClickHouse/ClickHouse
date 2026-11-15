-- Tags: zookeeper, no-replicated-database
-- zookeeper: the replicated arms need Keeper.
-- no-replicated-database: those arms name their Keeper path explicitly, which a `Replicated`
--   database rejects (`database_replicated_allow_replicated_engine_arguments` defaults to 0).

-- A part can hold a column that no table schema has: a partition detached before `DROP COLUMN` and
-- re-attached after it, or such a partition cloned into another table by `ATTACH PARTITION FROM`.
-- `MATERIALIZE TTL` on a table whose only TTL is a rows TTL and that has `ttl_only_drop_parts` takes
-- a fast path which marked every column it does not rewrite as ignored, and an ignored column
-- skipped the check for columns absent from the table: the mutation read the column, its identifier
-- did not resolve any more, and the mutation failed with `UNKNOWN_IDENTIFIER` and kept being
-- retried, wedging the table's mutation queue. A cloned part carries the destination's metadata
-- version, where the same state instead reached the logical error for a part that is not behind its
-- table, which aborts a debug or sanitizer build and replays from the queue on restart.

DROP TABLE IF EXISTS t_05210;
DROP TABLE IF EXISTS t_05210_replicated;
DROP TABLE IF EXISTS t_05210_src;
DROP TABLE IF EXISTS t_05210_dst;

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

-- The same partition on a replicated table, where the re-attached part keeps the metadata version it
-- was written at and the table has moved on to the version that dropped the column.
CREATE TABLE t_05210_replicated (id UInt64, val UInt64, p UInt8, ts DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_05210_replicated', '1')
PARTITION BY p ORDER BY id TTL ts + INTERVAL 30 YEAR
SETTINGS ttl_only_drop_parts = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
ALTER TABLE t_05210_replicated ADD COLUMN c UInt32;
INSERT INTO t_05210_replicated SELECT number, number, 1, now(), 42 FROM numbers(100);

ALTER TABLE t_05210_replicated DETACH PARTITION 1;
ALTER TABLE t_05210_replicated DROP COLUMN c;
ALTER TABLE t_05210_replicated ATTACH PARTITION 1;

-- Both ALTERs bumped it, and the part was written at version 1, so the part is behind the table.
SELECT 'replicated: the table metadata version', metadata_version FROM system.tables
WHERE database = currentDatabase() AND name = 't_05210_replicated';
SELECT 'replicated: the part still has the dropped column', countIf(column = 'c') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_05210_replicated' AND active;

ALTER TABLE t_05210_replicated MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT 'replicated: rows after materializing the TTL', count(), sum(val) FROM t_05210_replicated;
SELECT 'replicated: the rewrite dropped the column', countIf(column = 'c') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_05210_replicated' AND active;
SELECT 'replicated: unfinished mutations', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_05210_replicated' AND NOT is_done;

DROP TABLE t_05210_replicated;

-- The same partition cloned into another table by `ATTACH PARTITION FROM`. The clone is stamped with
-- the destination's metadata version while it keeps the source's columns, and the destination is
-- never altered, so the part is not behind the table and the column is in neither schema.
CREATE TABLE t_05210_src (id UInt64, val UInt64, p UInt8, ts DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_05210_src', '1')
PARTITION BY p ORDER BY id TTL ts + INTERVAL 30 YEAR
SETTINGS ttl_only_drop_parts = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
ALTER TABLE t_05210_src ADD COLUMN c UInt32;
INSERT INTO t_05210_src SELECT number, number, 1, now(), 42 FROM numbers(100);

ALTER TABLE t_05210_src DETACH PARTITION 1;
ALTER TABLE t_05210_src DROP COLUMN c;
ALTER TABLE t_05210_src ATTACH PARTITION 1;

CREATE TABLE t_05210_dst (id UInt64, val UInt64, p UInt8, ts DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_05210_dst', '1')
PARTITION BY p ORDER BY id TTL ts + INTERVAL 30 YEAR
SETTINGS ttl_only_drop_parts = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
ALTER TABLE t_05210_dst ATTACH PARTITION 1 FROM t_05210_src;

SELECT 'cloned: the table metadata version', metadata_version FROM system.tables
WHERE database = currentDatabase() AND name = 't_05210_dst';
SELECT 'cloned: the part still has the dropped column', countIf(column = 'c') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_05210_dst' AND active;

ALTER TABLE t_05210_dst MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT 'cloned: rows after materializing the TTL', count(), sum(val) FROM t_05210_dst;
SELECT 'cloned: the rewrite dropped the column', countIf(column = 'c') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_05210_dst' AND active;
SELECT 'cloned: unfinished mutations', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_05210_dst' AND NOT is_done;

DROP TABLE t_05210_src;
DROP TABLE t_05210_dst;
