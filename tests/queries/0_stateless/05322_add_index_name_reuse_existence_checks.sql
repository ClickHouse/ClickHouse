-- `ADD INDEX` is refused the name of an index whose files a part still holds, and the refusal keys
-- on what the statement actually does: `apply` resolves `IF NOT EXISTS` and `IF EXISTS` first, so a
-- no-op `ADD INDEX` is not refused and a no-op `DROP INDEX` does not excuse the one that follows it.

SET alter_sync = 0, mutations_sync = 0;

SELECT '-- adding an index that already exists is still a no-op';
DROP TABLE IF EXISTS t_ixe;
CREATE TABLE t_ixe (a UInt32, b UInt32, INDEX ix a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 128;

INSERT INTO t_ixe SELECT number, number FROM numbers(1000);

-- The part holds the files of `ix`, but they belong to the very index that owns the name.
ALTER TABLE t_ixe ADD INDEX IF NOT EXISTS ix b TYPE minmax GRANULARITY 1;
SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_ixe' AND name = 'ix';

SELECT '-- a name whose drop is still pending cannot be taken by a new index';
-- Keeps the `DROP INDEX` mutation pending, so the part keeps the files of the dropped index.
SYSTEM STOP MERGES t_ixe;
ALTER TABLE t_ixe DROP INDEX ix;
ALTER TABLE t_ixe ADD INDEX ix b TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- a no-op DROP INDEX in the same statement does not excuse the name';
KILL MUTATION WHERE database = currentDatabase() AND table = 't_ixe' FORMAT Null;
ALTER TABLE t_ixe DROP INDEX IF EXISTS ix, ADD INDEX ix b TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- the name is free again once no part holds its files';
SYSTEM START MERGES t_ixe;
OPTIMIZE TABLE t_ixe FINAL;
ALTER TABLE t_ixe ADD INDEX ix b TYPE minmax GRANULARITY 1;
SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_ixe' AND name = 'ix';

DROP TABLE t_ixe;

SELECT '-- re-adding a column while its drop is pending keeps working';
DROP TABLE IF EXISTS t_ixe2;
CREATE TABLE t_ixe2 (a UInt32, c UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 128, add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_ixe2 SELECT number, number FROM numbers(1000);

-- The implicit minmax index of `c` follows the column: it is dropped and re-added under the same
-- name while the pending `DROP COLUMN` still marks the files in the part stale, so the `ALTER` must
-- not be refused the way an explicit index of a name a part still holds files under is.
SYSTEM STOP MERGES t_ixe2;
ALTER TABLE t_ixe2 DROP COLUMN c;
ALTER TABLE t_ixe2 ADD COLUMN c UInt32;
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_ixe2' ORDER BY name;
-- The values of the dropped column stay hidden, so the re-added one reads as its default.
SELECT sum(c) FROM t_ixe2;

DROP TABLE t_ixe2;
