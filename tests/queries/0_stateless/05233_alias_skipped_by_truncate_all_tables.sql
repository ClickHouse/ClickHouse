-- Tags: no-replicated-database
-- no-replicated-database: TRUNCATE ALL TABLES is not supported for Replicated databases.

-- A database-wide TRUNCATE enqueues one task per table name, all sharing one query id. An `Alias` is
-- a name without data, so the alias entry and its target entry converge on one storage and both take
-- that storage's lock, which `RWLockImpl` rejects for a query id that already holds it.
--
-- The target must stay non-MergeTree for this to be covered: two exclusive acquisitions abort, two
-- shared ones are ref-counted. `Join` is deliberate: the test runner rewrites
-- `Log`/`TinyLog`/`StripeLog`/`Memory` to `MergeTree` in some lanes, which would make this vacuous.

CREATE TABLE j (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO j SELECT number, number FROM numbers(300);

CREATE TABLE al_1 ENGINE = Alias(currentDatabase(), 'j');
CREATE TABLE al_2 ENGINE = Alias(currentDatabase(), 'j');
CREATE TABLE al_3 ENGINE = Alias(currentDatabase(), 'j');
CREATE TABLE al_4 ENGINE = Alias(currentDatabase(), 'j');
CREATE TABLE al_5 ENGINE = Alias(currentDatabase(), 'j');
CREATE TABLE al_6 ENGINE = Alias(currentDatabase(), 'j');
CREATE TABLE al_7 ENGINE = Alias(currentDatabase(), 'j');
CREATE TABLE al_8 ENGINE = Alias(currentDatabase(), 'j');

-- Without this an arm could report the expected value while probing a plain table instead of an alias.
SELECT '-- fixture';
SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name;

SELECT '-- truncate all tables: the target is emptied by its own entry';
TRUNCATE ALL TABLES FROM {CLICKHOUSE_DATABASE:Identifier};
SELECT count() FROM j;

SELECT '-- an alias is not a data holder: truncating only the aliases truncates nothing';
INSERT INTO j SELECT number, number FROM numbers(300);
TRUNCATE TABLES FROM {CLICKHOUSE_DATABASE:Identifier} LIKE 'al%';
SELECT count() FROM j;

SELECT '-- an alias to a missing table no longer fails the whole statement';
CREATE TABLE al_missing ENGINE = Alias(currentDatabase(), 'no_such_table');
TRUNCATE ALL TABLES FROM {CLICKHOUSE_DATABASE:Identifier};
SELECT count() FROM j;
