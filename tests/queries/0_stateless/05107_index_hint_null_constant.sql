-- https://github.com/ClickHouse/ClickHouse/issues/112116
-- A hint atom that folds to a constant `NULL` tells index analysis nothing, but converting it to the
-- hint's non-nullable result type threw, so wrapping a working predicate in `indexHint` - documented
-- as a row-level no-op - turned the query into an exception.

DROP TABLE IF EXISTS t_hint_null;
CREATE TABLE t_hint_null (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_hint_null SELECT number FROM numbers(100);

SELECT count() FROM t_hint_null WHERE id = 16 AND toInt64OrNull('x');
SELECT count() FROM t_hint_null WHERE indexHint(id = 16 AND toInt64OrNull('x')) AND (id = 16 AND toInt64OrNull('x'));
SELECT count() FROM t_hint_null WHERE indexHint(CAST(NULL, 'Nullable(UInt8)'));
SELECT count() FROM t_hint_null WHERE indexHint(NULL);
SELECT count() FROM t_hint_null WHERE indexHint(CAST(NULL, 'Nullable(Int64)'));
-- An unsatisfiable hint prunes everything, which is what the user asserted with it. `NULL` behaves
-- like the `0` and the always-false predicate below, instead of failing the query.
SELECT count() FROM t_hint_null WHERE indexHint(NULL) AND id = 16;
SELECT count() FROM t_hint_null WHERE indexHint(0) AND id = 16;
SELECT count() FROM t_hint_null WHERE indexHint(id > 1000000) AND id = 16;
SELECT count() FROM t_hint_null WHERE indexHint(id = 16 OR NULL) AND id = 16;

SELECT 'controls';
SELECT count() FROM t_hint_null WHERE indexHint(toNullable(1));
SELECT count() FROM t_hint_null WHERE indexHint(toInt64OrNull(toString(id)));
SELECT count() FROM t_hint_null WHERE indexHint(id = 16) AND id = 16;
SELECT count() FROM t_hint_null WHERE indexHint(id < 10) AND id < 10;
SELECT count() FROM t_hint_null WHERE id = 16;

SELECT 'the hint still narrows the read';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT id FROM t_hint_null WHERE indexHint(id < 10)) WHERE explain LIKE '%Granules: 1/%';

DROP TABLE t_hint_null;
