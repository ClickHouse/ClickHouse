-- https://github.com/ClickHouse/ClickHouse/issues/112116
-- A hint atom that folds to a constant `NULL` tells index analysis nothing, but converting it to the
-- hint's non-nullable result type threw, so wrapping a working predicate in `indexHint` - documented
-- as a row-level no-op - turned the query into an exception.

DROP TABLE IF EXISTS t_hint_null;
CREATE TABLE t_hint_null (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_hint_null SELECT number FROM numbers(100);

SELECT count() FROM t_hint_null WHERE id = 16 AND toInt64OrNull('x');
SELECT count() FROM t_hint_null WHERE indexHint(id = 16 AND toInt64OrNull('x')) AND (id = 16 AND toInt64OrNull('x'));
-- A hint that carries no information must not fail the query. How many rows a bare `count()` over it
-- reports depends on which read path serves the count, so only require that it runs.
SELECT count() FROM t_hint_null WHERE indexHint(CAST(NULL, 'Nullable(UInt8)')) FORMAT Null;
SELECT count() FROM t_hint_null WHERE indexHint(NULL) FORMAT Null;
SELECT count() FROM t_hint_null WHERE indexHint(CAST(NULL, 'Nullable(Int64)')) FORMAT Null;
SELECT count() FROM t_hint_null WHERE indexHint(NULL) AND id < 1000000000;
SELECT count() FROM t_hint_null WHERE indexHint(CAST(NULL, 'Nullable(UInt8)')) AND id < 1000000000;
-- An unsatisfiable hint prunes everything, which is what the user asserted with it. `NULL` behaves
-- like the `0` and the always-false predicate below, instead of failing the query.
SELECT count() FROM t_hint_null WHERE indexHint(NULL) AND id = 16;
SELECT count() FROM t_hint_null WHERE indexHint(0) AND id = 16;
SELECT count() FROM t_hint_null WHERE indexHint(id > 1000000) AND id = 16;
SELECT count() FROM t_hint_null WHERE indexHint(id = 16 OR NULL) AND id = 16;

SELECT 'a truthy constant that does not fit UInt8';
-- https://github.com/ClickHouse/ClickHouse/issues/111684
DROP TABLE IF EXISTS t_hint_truthy;
CREATE TABLE t_hint_truthy (g UInt16, id UInt32) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
INSERT INTO t_hint_truthy SELECT number % 10, number FROM numbers(1000);
SELECT count() FROM t_hint_truthy WHERE (id >= 1 AND id <= 3) AND indexHint(256);
SELECT count() FROM t_hint_truthy WHERE (id >= 1 AND id <= 3) AND indexHint(toNullable(256));
SELECT count() FROM t_hint_truthy WHERE (id >= 1 AND id <= 3) AND indexHint(toUInt16(256));
SELECT count() FROM t_hint_truthy WHERE (id >= 1 AND id <= 3) AND indexHint(1);
SELECT count() FROM t_hint_truthy WHERE (id >= 1 AND id <= 3) AND indexHint(materialize(256));
SELECT count() FROM t_hint_truthy WHERE (id >= 1 AND id <= 3);

SELECT 'a Nothing-typed argument';
-- https://github.com/ClickHouse/ClickHouse/issues/111685
SELECT count() FROM t_hint_truthy WHERE (id >= 1 AND id <= 3) AND indexHint(assumeNotNull(materialize(NULL)));
SELECT count() FROM t_hint_truthy WHERE indexHint(materialize(id < 10)) AND id < 10;
SELECT count() FROM t_hint_truthy WHERE indexHint(id < 10) AND id < 10;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT id FROM t_hint_truthy WHERE indexHint(id < 10)) WHERE explain LIKE '%Granules: 1/16%';
DROP TABLE t_hint_truthy;

SELECT 'controls';
SELECT count() FROM t_hint_null WHERE indexHint(toNullable(1));
SELECT count() FROM t_hint_null WHERE indexHint(toInt64OrNull(toString(id)));
SELECT count() FROM t_hint_null WHERE indexHint(id = 16) AND id = 16;
SELECT count() FROM t_hint_null WHERE indexHint(id < 10) AND id < 10;
SELECT count() FROM t_hint_null WHERE id = 16;

SELECT 'the hint still narrows the read';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT id FROM t_hint_null WHERE indexHint(id < 10)) WHERE explain LIKE '%Granules: 1/%';

DROP TABLE t_hint_null;
