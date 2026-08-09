-- `ALTER TABLE ... ADD COLUMN` / `MODIFY COLUMN` build the explicit column type in
-- `AlterCommand::parse`, bypassing `InterpreterCreateQuery`. The current state version has to be
-- pinned into the stored metadata on this path too, the same way `CREATE TABLE` does it, or the
-- column would keep the version 0 layout and lose its skip degree on every local storage round
-- trip, making a merge over a lopsided split give a wrong, split-dependent answer.

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

-- `MODIFY COLUMN` with an explicit type takes the same path. A column created with the explicit
-- version 0 keeps the pre-versioning layout, and restating the type unversioned pins it to the
-- current version, so states stored after the `ALTER` keep their skip degree.

DROP TABLE IF EXISTS quantile_deterministic_alter_modify;

CREATE TABLE quantile_deterministic_alter_modify (part UInt8, state AggregateFunction(0, quantileDeterministic, UInt64, UInt64))
ENGINE = MergeTree ORDER BY part;

SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 'quantile_deterministic_alter_modify' AND name = 'state';

ALTER TABLE quantile_deterministic_alter_modify MODIFY COLUMN state AggregateFunction(quantileDeterministic, UInt64, UInt64);

SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 'quantile_deterministic_alter_modify' AND name = 'state';

DETACH TABLE quantile_deterministic_alter_modify;
ATTACH TABLE quantile_deterministic_alter_modify;

SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 'quantile_deterministic_alter_modify' AND name = 'state';

INSERT INTO quantile_deterministic_alter_modify SELECT 0, quantileDeterministicState(number, number) FROM numbers(990000);
INSERT INTO quantile_deterministic_alter_modify SELECT 1, quantileDeterministicState(number + 990000, number + 990000) FROM numbers(10000);

SELECT quantileDeterministicMerge(state) FROM quantile_deterministic_alter_modify;

DROP TABLE quantile_deterministic_alter_modify;
