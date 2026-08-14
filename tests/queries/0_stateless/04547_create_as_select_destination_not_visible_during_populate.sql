-- Regression test for the plain `CREATE TABLE ... AS SELECT` temporary-table publish path (issue #26746).
-- The fix routes a plain `CREATE TABLE ... AS SELECT` on an Atomic database through a temporary table that
-- is only published under the final name by the trailing RENAME. As a documented, intentional consequence,
-- the populating `SELECT` runs while the destination does not yet exist: the table becomes visible only once
-- it has been fully populated. This test pins that visibility contract so the change stays deliberate.

DROP TABLE IF EXISTS dst;

-- The destination is not registered while the populate runs, so `system.tables` does not see it yet:
-- the count is 0 (with the previous create-then-populate order it would have been 1).
CREATE TABLE dst ENGINE = Memory AS
SELECT count() AS c FROM system.tables WHERE database = currentDatabase() AND name = 'dst';
SELECT 'system_tables_during_populate', c FROM dst;
DROP TABLE dst;

-- Likewise, a `SELECT` that reads the destination itself fails with `UNKNOWN_TABLE` instead of reading the
-- just-created empty table (which used to yield an empty table).
SELECT 'self_reference';
CREATE TABLE dst ENGINE = Memory AS SELECT * FROM dst; -- { serverError UNKNOWN_TABLE }
SELECT 'no_orphan_after_self_reference', count() FROM system.tables WHERE database = currentDatabase() AND name = 'dst';

-- Sanity: a normal `CREATE TABLE ... AS SELECT` that does not reference the destination is unaffected and
-- populates as expected.
CREATE TABLE dst ENGINE = Memory AS SELECT number AS n FROM numbers(3);
SELECT 'normal_populate', count(), sum(n) FROM dst;
DROP TABLE dst;
