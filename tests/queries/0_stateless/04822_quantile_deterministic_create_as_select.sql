-- `CREATE TABLE ... AS SELECT` infers its columns from the query sample block. A fresh
-- `quantileDeterministic` state type already spells its version out, but the inferred types are
-- pinned too, so that a table inferred from an unversioned source (e.g. a `SELECT` from an old
-- table) does not keep the version 0 layout and lose its skip degree on every local storage round
-- trip, making a merge over a lopsided split give a wrong, split-dependent answer.

DROP TABLE IF EXISTS quantile_deterministic_create_as_select;

-- A very lopsided split: 990000 rows in one state, 10000 in the other.
CREATE TABLE quantile_deterministic_create_as_select
ENGINE = MergeTree ORDER BY part AS
SELECT intDiv(number, 990000) AS part, medianDeterministicState(number, number) AS state
FROM numbers(1000000)
GROUP BY part;

-- The pinned state version reaches the inferred column type in the stored metadata.
SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 'quantile_deterministic_create_as_select' AND name = 'state';

-- And it survives a reload of the table from its metadata.
DETACH TABLE quantile_deterministic_create_as_select;
ATTACH TABLE quantile_deterministic_create_as_select;

SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 'quantile_deterministic_create_as_select' AND name = 'state';

-- The merge over the lopsided split must match the value a single state over all the rows gives (492708).
SELECT medianDeterministic(number, number) FROM numbers(1000000);
SELECT medianDeterministicMerge(state) FROM quantile_deterministic_create_as_select;

DROP TABLE quantile_deterministic_create_as_select;

-- A materialized view without an explicit column list infers the columns of its inner table the
-- same way, so the pin has to reach it too.

DROP TABLE IF EXISTS quantile_deterministic_mv_source;
DROP TABLE IF EXISTS quantile_deterministic_mv;

CREATE TABLE quantile_deterministic_mv_source (part UInt8, value UInt64) ENGINE = MergeTree ORDER BY part;

CREATE MATERIALIZED VIEW quantile_deterministic_mv
ENGINE = AggregatingMergeTree ORDER BY part AS
SELECT part, medianDeterministicState(value, value) AS state
FROM quantile_deterministic_mv_source
GROUP BY part;

INSERT INTO quantile_deterministic_mv_source SELECT 0, number FROM numbers(990000);
INSERT INTO quantile_deterministic_mv_source SELECT 1, number + 990000 FROM numbers(10000);

-- The inner table stores the states at the pinned version.
SELECT toTypeName(state) FROM quantile_deterministic_mv LIMIT 1;

-- The fixed merge is split-independent, so the lopsided split must give the single-state value.
SELECT medianDeterministicMerge(state) FROM quantile_deterministic_mv;

DROP TABLE quantile_deterministic_mv;
DROP TABLE quantile_deterministic_mv_source;
