-- `KILL MUTATION` must not forget a pending `DROP INDEX` whose name the metadata has since given to
-- another index. The parts the mutation has not rewritten still hold the dropped index's granules,
-- and `AlterConversions` reports them as stale only while the drop is pending, so forgetting the
-- mutation would hand those granules to the new index of the same name.

SET alter_sync = 0, mutations_sync = 0;

DROP TABLE IF EXISTS t_ixk;
CREATE TABLE t_ixk (a UInt32, b UInt32, INDEX ix a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 128;

-- The values of `a` are above all values of `b`, so granules of the old index never contain them.
INSERT INTO t_ixk SELECT number + 1000000, 999999 - number FROM numbers(10000);

-- Keeps the `DROP INDEX` mutation pending.
SYSTEM STOP MERGES t_ixk;

ALTER TABLE t_ixk DROP INDEX ix, ADD INDEX ix b TYPE minmax GRANULARITY 1;

SELECT '-- the kill is refused while another index holds the name';
KILL MUTATION WHERE database = currentDatabase() AND table = 't_ixk'; -- { serverError BAD_ARGUMENTS }

SELECT '-- the drop stayed pending, so the old granules are still kept out of the way';
SELECT count() FROM t_ixk WHERE b = 995000;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_ixk' AND NOT is_done;

DROP TABLE t_ixk;

SELECT '-- a drop whose name nothing has taken is killed as before';
DROP TABLE IF EXISTS t_ixk2;
CREATE TABLE t_ixk2 (a UInt32, INDEX ix a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 128;

INSERT INTO t_ixk2 SELECT number FROM numbers(1000);

SYSTEM STOP MERGES t_ixk2;

ALTER TABLE t_ixk2 DROP INDEX ix;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_ixk2' FORMAT Null;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_ixk2' AND NOT is_done;

DROP TABLE t_ixk2;

SELECT '-- re-adding a name whose killed drop left files behind is refused';
DROP TABLE IF EXISTS t_ixk3;
CREATE TABLE t_ixk3 (a UInt32, b UInt32, INDEX ix a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 128;

INSERT INTO t_ixk3 SELECT number + 1000000, 999999 - number FROM numbers(10000);

-- Keeps the `DROP INDEX` mutation pending, so killing it leaves the index files on the part.
SYSTEM STOP MERGES t_ixk3;

ALTER TABLE t_ixk3 DROP INDEX ix;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_ixk3' FORMAT Null;

ALTER TABLE t_ixk3 ADD INDEX ix b TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- a name nothing left files under is unaffected';
ALTER TABLE t_ixk3 ADD INDEX ix2 b TYPE minmax GRANULARITY 1;
SELECT count() FROM t_ixk3 WHERE b = 995000;

DROP TABLE t_ixk3;
