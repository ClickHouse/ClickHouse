-- Lexicographic tuple comparison `(k1, k2, ...) <op> (c1, c2, ...)` used for index pruning (issue #75086).
--
-- Correctness is checked by comparing the result of the tuple comparison against its manually expanded
-- equivalent `k1 < c1 OR (k1 = c1 AND ...)`, which is already handled by the index. Because the index can
-- only prune granules (the predicate is still evaluated per row), the final result must be identical; a
-- mismatch would mean an unsound (granule-dropping) optimization. Granule pruning itself is checked with
-- `EXPLAIN indexes = 1`.

DROP TABLE IF EXISTS t_tuple_lex;

CREATE TABLE t_tuple_lex (a UInt32, b UInt32, c UInt32, d UInt32)
ENGINE = MergeTree ORDER BY (a, b, c) SETTINGS index_granularity = 4;

-- 64 rows, physically sorted as (a, b, c) = base-4 digits of `number`, i.e. 16 granules of 4 rows each.
-- `d` is a non-key column used to exercise prefix relaxation.
INSERT INTO t_tuple_lex SELECT intDiv(number, 16), intDiv(number, 4) % 4, number % 4, number % 3 FROM numbers(64);

SELECT '-- correctness: tuple comparison vs manual expansion (must all be 1) --';

-- 2-element, all operators.
SELECT (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE (a, b) <  (2, 1))
     = (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE a < 2 OR (a = 2 AND b <  1));
SELECT (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE (a, b) <= (2, 1))
     = (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE a < 2 OR (a = 2 AND b <= 1));
SELECT (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE (a, b) >  (2, 1))
     = (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE a > 2 OR (a = 2 AND b >  1));
SELECT (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE (a, b) >= (2, 1))
     = (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE a > 2 OR (a = 2 AND b >= 1));

-- 3-element.
SELECT (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE (a, b, c) <  (2, 1, 3))
     = (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE a < 2 OR (a = 2 AND (b < 1 OR (b = 1 AND c <  3))));
SELECT (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE (a, b, c) >= (1, 2, 0))
     = (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE a > 1 OR (a = 1 AND (b > 2 OR (b = 2 AND c >= 0))));

-- Constant on the left (operator is flipped).
SELECT (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE (2, 1) > (a, b))
     = (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE a < 2 OR (a = 2 AND b < 1));

-- Prefix where the second element `d` is not a key column, so only the `a` prefix is used for pruning
-- (relaxed to `a <= 2`); the result is still exact.
SELECT (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE (a, d) < (2, 1))
     = (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE a < 2 OR (a = 2 AND d < 1));

-- Float constant against integer key (exact via float-to-int boundary handling on the prefix).
SELECT (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE (a, b) < (2.0, 1.0))
     = (SELECT sum(sipHash64(a, b, c)) FROM t_tuple_lex WHERE a < 2 OR (a = 2 AND b < 1));

SELECT '-- pruning: EXPLAIN granules with the optimization ON --';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tuple_lex WHERE (a, b) < (2, 1)) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%';

SELECT '-- pruning: same query with the optimization OFF reads all granules --';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tuple_lex WHERE (a, b) < (2, 1) SETTINGS analyze_index_with_tuple_lexicographic_comparison = 0) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%';

SELECT '-- pruning: 3-element greater --';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tuple_lex WHERE (a, b, c) > (2, 1, 3)) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%';

DROP TABLE t_tuple_lex;

-- Nullable and LowCardinality keys: the result must stay exact (granules with NULL are handled conservatively).
DROP TABLE IF EXISTS t_tuple_lex_null;
CREATE TABLE t_tuple_lex_null (a LowCardinality(String), b Nullable(UInt32))
ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 4, allow_nullable_key = 1;
INSERT INTO t_tuple_lex_null SELECT toString(intDiv(number, 8)), if(number % 5 = 0, NULL, number % 8) FROM numbers(64);

SELECT '-- correctness with Nullable / LowCardinality keys (must all be 1) --';
SELECT (SELECT sum(sipHash64(a, b)) FROM t_tuple_lex_null WHERE (a, b) <  ('4', 3))
     = (SELECT sum(sipHash64(a, b)) FROM t_tuple_lex_null WHERE a < '4' OR (a = '4' AND b <  3));
SELECT (SELECT sum(sipHash64(a, b)) FROM t_tuple_lex_null WHERE (a, b) >= ('4', 3))
     = (SELECT sum(sipHash64(a, b)) FROM t_tuple_lex_null WHERE a > '4' OR (a = '4' AND b >= 3));
DROP TABLE t_tuple_lex_null;

-- Monotonic function chain on the first tuple element (toDate over a DateTime key column).
DROP TABLE IF EXISTS t_tuple_lex_mono;
CREATE TABLE t_tuple_lex_mono (t DateTime, id UInt32)
ENGINE = MergeTree ORDER BY (t, id) SETTINGS index_granularity = 4;
INSERT INTO t_tuple_lex_mono SELECT toDateTime('2023-01-01 00:00:00') + number * 3600, number % 7 FROM numbers(64);

SELECT '-- correctness with a monotonic function chain (must be 1) --';
SELECT (SELECT sum(sipHash64(t, id)) FROM t_tuple_lex_mono WHERE (toDate(t), id) < (toDate('2023-01-02'), 3))
     = (SELECT sum(sipHash64(t, id)) FROM t_tuple_lex_mono WHERE toDate(t) < toDate('2023-01-02') OR (toDate(t) = toDate('2023-01-02') AND id < 3));
DROP TABLE t_tuple_lex_mono;

-- minmax skip index: the same KeyCondition path, so the tuple comparison prunes minmax-indexed granules.
DROP TABLE IF EXISTS t_tuple_lex_minmax;
CREATE TABLE t_tuple_lex_minmax (id UInt32, a UInt32, b UInt32, INDEX mm (a, b) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;
INSERT INTO t_tuple_lex_minmax SELECT number, intDiv(number, 16), intDiv(number, 4) % 4 FROM numbers(64);

SELECT '-- correctness with a minmax skip index (must be 1) --';
SELECT (SELECT sum(sipHash64(id, a, b)) FROM t_tuple_lex_minmax WHERE (a, b) < (2, 1))
     = (SELECT sum(sipHash64(id, a, b)) FROM t_tuple_lex_minmax WHERE a < 2 OR (a = 2 AND b < 1));

SELECT '-- pruning: minmax skip index granules with the optimization ON --';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tuple_lex_minmax WHERE (a, b) < (2, 1)) WHERE explain LIKE '%Skip%' OR explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
DROP TABLE t_tuple_lex_minmax;
