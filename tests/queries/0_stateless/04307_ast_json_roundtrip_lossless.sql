-- Round-trip tests for `parseQueryToJSON` / `formatQueryFromJSON` that previously
-- lost semantically-significant AST state, plus AST types that previously had no
-- JSON serializer at all. Each `SELECT formatQueryFromJSON(parseQueryToJSON('...'))`
-- must reproduce the original query (modulo canonical formatting).

-- Lossy fields that are now preserved:
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`id` UInt64) ENGINE = MergeTree ORDER BY id UNIQUE KEY id'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TEMPORARY TABLE t (`x` UInt8) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('DELETE FROM t ON CLUSTER c WHERE id = 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t ON CLUSTER c FINAL'));
SELECT formatQueryFromJSON(parseQueryToJSON('SHOW TABLES FORMAT JSON'));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM DROP DATABASE REPLICA \'r\' FROM DATABASE db WITH TABLES'));

-- Column aliases of a derived table (Expression::ALIASES) survive:
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM (SELECT 1, 2) AS x(a, b)'));

-- Non-finite Float64 literals are JSON-valid and round-trip:
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT nan'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT inf'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT -inf'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT [1., nan, inf]'));

-- Parameterized identifiers (`{name:Identifier}`) survive instead of becoming empty:
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE {tbl:Identifier} (`x` UInt8) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('DROP TABLE {tbl:Identifier}'));
SELECT formatQueryFromJSON(parseQueryToJSON('RENAME TABLE {a:Identifier} TO {b:Identifier}'));

-- Previously-unregistered AST types now round-trip:
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE INDEX idx ON t (col) TYPE minmax GRANULARITY 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('DROP INDEX idx ON t'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE VIEW v DEFINER = user SQL SECURITY DEFINER AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t STREAM'));

-- Two-argument form: the original suffix must NOT reintroduce clauses the JSON AST
-- dropped, but trailing comments/whitespace are still preserved.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1'), 'SELECT 1 FORMAT JSON');
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 /* c */ + 2'), 'SELECT 1 /* c */ + 2');
