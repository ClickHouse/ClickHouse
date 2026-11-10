-- `ALTER TABLE ... ADD COLUMN` builds the explicit column type in `AlterCommand::parse`, bypassing
-- `InterpreterCreateQuery`. The current state version has to be pinned into the stored metadata on
-- this path too, the same way `CREATE TABLE` does it, or the column would keep the version 0 layout
-- and lose its skip degree on every local storage round trip, making a merge over a lopsided split
-- give a wrong, split-dependent answer.

DROP TABLE IF EXISTS quantile_deterministic_alter_add;

CREATE TABLE quantile_deterministic_alter_add (part UInt8) ENGINE = MergeTree ORDER BY part;
ALTER TABLE quantile_deterministic_alter_add ADD COLUMN state AggregateFunction(medianDeterministic, UInt64, UInt64);

-- The pinned state version reaches the stored metadata.
SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 'quantile_deterministic_alter_add' AND name = 'state';

-- And it survives a reload of the table from its metadata.
DETACH TABLE quantile_deterministic_alter_add;
ATTACH TABLE quantile_deterministic_alter_add;

SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 'quantile_deterministic_alter_add' AND name = 'state';

-- A very lopsided split: 990000 rows in one state, 10000 in the other.
INSERT INTO quantile_deterministic_alter_add SELECT 0, medianDeterministicState(number, number) FROM numbers(990000);
INSERT INTO quantile_deterministic_alter_add SELECT 1, medianDeterministicState(number + 990000, number + 990000) FROM numbers(10000);

-- The merge over the lopsided split must match the value a single state over all the rows gives (492708).
SELECT medianDeterministic(number, number) FROM numbers(1000000);
SELECT medianDeterministicMerge(state) FROM quantile_deterministic_alter_add;

DROP TABLE quantile_deterministic_alter_add;

-- `MODIFY COLUMN`, unlike `ADD COLUMN`, must NOT pin the version implicitly: it can be applied to a
-- column that already holds data, a state version change is a metadata-only conversion (the version
-- is not part of `DataTypeAggregateFunction::equals`), and a rewrite could not repair the old states
-- anyway - a version 0 state does not carry its skip degree. So restating the type unversioned leaves
-- the column at the version it already had, and the stored data keeps matching the metadata.

DROP TABLE IF EXISTS quantile_deterministic_alter_modify;

CREATE TABLE quantile_deterministic_alter_modify (part UInt8, state AggregateFunction(0, quantileDeterministic, UInt64, UInt64))
ENGINE = MergeTree ORDER BY part;

-- Rows written before the `ALTER`, in the version 0 layout: their skip degree is already lost, so the
-- lopsided merge gives the wrong, split-dependent 506014 instead of 492708. No `ALTER` can fix that.
INSERT INTO quantile_deterministic_alter_modify SELECT 0, quantileDeterministicState(number, number) FROM numbers(990000);
INSERT INTO quantile_deterministic_alter_modify SELECT 1, quantileDeterministicState(number + 990000, number + 990000) FROM numbers(10000);

SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 'quantile_deterministic_alter_modify' AND name = 'state';
SELECT quantileDeterministicMerge(state) FROM quantile_deterministic_alter_modify;

ALTER TABLE quantile_deterministic_alter_modify MODIFY COLUMN state AggregateFunction(quantileDeterministic, UInt64, UInt64);

-- Still version 0, and the pre-existing states read back exactly as before.
SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 'quantile_deterministic_alter_modify' AND name = 'state';
SELECT quantileDeterministicMerge(state) FROM quantile_deterministic_alter_modify;

-- The current layout can still be requested explicitly. Parts written earlier record their own column
-- types in `columns.txt`, so they keep being read with the version 0 layout and stay readable, also
-- through the merge that rewrites them into the new one.
ALTER TABLE quantile_deterministic_alter_modify MODIFY COLUMN state AggregateFunction(1, quantileDeterministic, UInt64, UInt64);

SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 'quantile_deterministic_alter_modify' AND name = 'state';
SELECT quantileDeterministicMerge(state) FROM quantile_deterministic_alter_modify;

OPTIMIZE TABLE quantile_deterministic_alter_modify FINAL;
SELECT quantileDeterministicMerge(state) FROM quantile_deterministic_alter_modify;

DETACH TABLE quantile_deterministic_alter_modify;
ATTACH TABLE quantile_deterministic_alter_modify;

SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 'quantile_deterministic_alter_modify' AND name = 'state';

-- States written after the explicit `ALTER` do keep their skip degree.
TRUNCATE TABLE quantile_deterministic_alter_modify;
INSERT INTO quantile_deterministic_alter_modify SELECT 0, quantileDeterministicState(number, number) FROM numbers(990000);
INSERT INTO quantile_deterministic_alter_modify SELECT 1, quantileDeterministicState(number + 990000, number + 990000) FROM numbers(10000);

SELECT quantileDeterministicMerge(state) FROM quantile_deterministic_alter_modify;

DROP TABLE quantile_deterministic_alter_modify;
