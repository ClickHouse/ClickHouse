-- Tags: no-parallel-replicas
-- no-parallel-replicas: dictionaries created in the test database are not
-- propagated to parallel replica worker nodes, so `dictGetOrNull('d_73633', ...)`
-- fails with `BAD_ARGUMENTS: Dictionary not found` on the workers.

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/73633
--
-- `dictGetOrNull` with a `Nullable` key column was silently overwriting other
-- columns in the SELECT projection with `NULL` whenever a key was missing in
-- the dictionary. Root cause: `FunctionDictGetNoType::executeImpl` calls
-- `wrapInNullable`, which produces a `ColumnNullable` whose null map shares
-- storage with the input key column's null map. `FunctionDictGetOrNull` then
-- mutated that null map in place via `addNullMap`, corrupting the input
-- column. The fix uses `IColumn::mutate` to deep-clone any shared sub-columns
-- before mutation.

DROP DICTIONARY IF EXISTS d_73633;
DROP TABLE IF EXISTS t_73633_memory;
DROP TABLE IF EXISTS t_73633_mt;

CREATE DICTIONARY d_73633 (id UInt64, name String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(QUERY $$SELECT c1 AS id, c2 AS name FROM VALUES((1, 'one'), (2, 'two'))$$))
LAYOUT(FLAT()) LIFETIME(0);

SELECT 'subquery + arrayJoin';
SELECT x, dictGetOrNull('d_73633', 'name', x) AS dx
FROM (SELECT toNullable(arrayJoin([0, 1, 2])) AS x)
ORDER BY x;

CREATE TABLE t_73633_memory (x Nullable(UInt64)) ENGINE = Memory;
INSERT INTO t_73633_memory VALUES (0), (1), (2);

SELECT 'Memory engine';
SELECT x, dictGetOrNull('d_73633', 'name', x) AS dx
FROM t_73633_memory
ORDER BY x;

SELECT 'Memory engine with ifNull wrapper';
SELECT x, dictGetOrNull('d_73633', 'name', ifNull(x, 0)) AS dx
FROM t_73633_memory
ORDER BY x;

CREATE TABLE t_73633_mt (x Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_73633_mt VALUES (0), (1), (2);

SELECT 'MergeTree engine';
SELECT x, dictGetOrNull('d_73633', 'name', x) AS dx
FROM t_73633_mt
ORDER BY x;

SELECT 'MergeTree engine with ifNull wrapper';
SELECT x, dictGetOrNull('d_73633', 'name', ifNull(x, 0)) AS dx
FROM t_73633_mt
ORDER BY x;

-- Aggregate over `x` after `dictGetOrNull`. Before the fix, sum(x) returned 1
-- (only the row with x = 1 survived) because rows with x = 0 and x = 2 were
-- corrupted to NULL.
SELECT 'aggregate preserves x';
SELECT sum(x), count(dx)
FROM (
    SELECT x, dictGetOrNull('d_73633', 'name', x) AS dx
    FROM t_73633_memory
);

-- Same with the original reporter's reproducer shape — `Nullable(String)` JSON
-- column passed into `dictGetOrNull` via `JSONExtractInt`, where the third
-- argument is `Nullable(Int)` returned by `JSONExtractInt(Nullable(String))`.
DROP TABLE IF EXISTS t_73633_json;
CREATE TABLE t_73633_json (id Int, json_string Nullable(String))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_73633_json VALUES (1, '{"ProviderId":0,"Option":"All"}');

SELECT 'original reporter shape — JSON column preserved';
SELECT json_string AS all_string,
       dictGetOrNull('d_73633', 'name', JSONExtractInt(json_string, 'ProviderId')) AS provider_b
FROM t_73633_json;

DROP TABLE t_73633_json;
DROP TABLE t_73633_mt;
DROP TABLE t_73633_memory;
DROP DICTIONARY d_73633;
