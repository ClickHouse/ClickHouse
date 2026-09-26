-- Tags: no-parallel-replicas
-- no-parallel-replicas: the `plan_*` rows read `EXPLAIN indexes = 1`, which differs with parallel replicas.

-- `DataTypeTuple::equals` ignores whether a tuple has explicit names (an anonymous tuple compares by its
-- ordinal names `1`, `2`, ...), but `toJSONString` writes a named tuple as an object and an anonymous one as
-- an array. A set element whose tuple naming mode differs from the key column's must therefore not be run
-- through the key transform under its own type: the rendered value names a partition the row does not live
-- in. Every `partition_*` row must equal its `oracle_*` row.

SET session_timezone = 'Asia/Kolkata';

DROP TABLE IF EXISTS k_named;
DROP TABLE IF EXISTS oracle_named;
DROP TABLE IF EXISTS k_anon;
DROP TABLE IF EXISTS oracle_anon;

CREATE TABLE k_named (a Array(Tuple(`1` DateTime('UTC')))) ENGINE = MergeTree PARTITION BY toJSONString(a) ORDER BY tuple();
CREATE TABLE oracle_named (a Array(Tuple(`1` DateTime('UTC')))) ENGINE = Memory;
INSERT INTO k_named SELECT [tuple(toDateTime(1675195200, 'UTC'))];
INSERT INTO k_named SELECT [tuple(toDateTime(1700000000, 'UTC'))];
INSERT INTO oracle_named SELECT [tuple(toDateTime(1675195200, 'UTC'))];

CREATE TABLE k_anon (a Array(Tuple(DateTime('UTC')))) ENGINE = MergeTree PARTITION BY toJSONString(a) ORDER BY tuple();
CREATE TABLE oracle_anon (a Array(Tuple(DateTime('UTC')))) ENGINE = Memory;
INSERT INTO k_anon SELECT [tuple(toDateTime(1675195200, 'UTC'))];
INSERT INTO k_anon SELECT [tuple(toDateTime(1700000000, 'UTC'))];
INSERT INTO oracle_anon SELECT [tuple(toDateTime(1675195200, 'UTC'))];

-- Named key, anonymous element in another timezone.
SELECT 'oracle_named_anon_tz', count() FROM (SELECT a FROM oracle_named WHERE a IN (SELECT [tuple(toDateTime(1675195200))]));
SELECT 'partition_named_anon_tz', count() FROM (SELECT a FROM k_named WHERE a IN (SELECT [tuple(toDateTime(1675195200))]));

-- Named key, anonymous element in the key's timezone: only the naming mode differs.
SELECT 'oracle_named_anon', count() FROM (SELECT a FROM oracle_named WHERE a IN (SELECT [tuple(toDateTime(1675195200, 'UTC'))]));
SELECT 'partition_named_anon', count() FROM (SELECT a FROM k_named WHERE a IN (SELECT [tuple(toDateTime(1675195200, 'UTC'))]));

-- Anonymous key, named element.
SELECT 'oracle_anon_named', count() FROM (SELECT a FROM oracle_anon WHERE a IN (SELECT CAST([tuple(toDateTime(1675195200, 'UTC'))], 'Array(Tuple(`1` DateTime(\'UTC\')))')));
SELECT 'partition_anon_named', count() FROM (SELECT a FROM k_anon WHERE a IN (SELECT CAST([tuple(toDateTime(1675195200, 'UTC'))], 'Array(Tuple(`1` DateTime(\'UTC\')))')));

-- Controls with the same naming mode on both sides: the key is still used for pruning.
SELECT 'oracle_anon_anon_tz', count() FROM (SELECT a FROM oracle_anon WHERE a IN (SELECT [tuple(toDateTime(1675195200))]));
SELECT 'partition_anon_anon_tz', count() FROM (SELECT a FROM k_anon WHERE a IN (SELECT [tuple(toDateTime(1675195200))]));
SELECT 'partition_named_named', count() FROM (SELECT a FROM k_named WHERE a IN (SELECT CAST([tuple(toDateTime(1675195200, 'UTC'))], 'Array(Tuple(`1` DateTime(\'UTC\')))')));
SELECT 'plan_anon_anon_tz_prunes', countIf(trim(explain) = 'Parts: 1/2') > 0 FROM (EXPLAIN indexes = 1 SELECT a FROM k_anon WHERE a IN (SELECT [tuple(toDateTime(1675195200))]));
SELECT 'plan_named_named_prunes', countIf(trim(explain) = 'Parts: 1/2') > 0 FROM (EXPLAIN indexes = 1 SELECT a FROM k_named WHERE a IN (SELECT CAST([tuple(toDateTime(1675195200, 'UTC'))], 'Array(Tuple(`1` DateTime(\'UTC\')))')));

DROP TABLE k_named;
DROP TABLE oracle_named;
DROP TABLE k_anon;
DROP TABLE oracle_anon;
