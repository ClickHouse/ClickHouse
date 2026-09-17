-- https://github.com/ClickHouse/ClickHouse/issues/116930
-- A conjunct extracted for one join side is pushed below the join while the original filter stays
-- above it, so it is evaluated twice. For a non-deterministic conjunct the two evaluations disagree:
-- `(t1.b + rand()) % 2 = 0` became two independent coin flips per row, so about half of the rows
-- that the filter accepts above the join were already dropped below it and the query returned fewer
-- rows than the same query with the push-down disabled.

-- The changed code path only runs when `use_join_disjunctions_push_down` is enabled and
-- `clickhouse-test` randomizes that setting. The runner passes randomized settings on the client
-- command line, which are per-query settings and outrank a `SET` issued earlier in the session, so
-- every query below pins the setting in its own `SETTINGS` clause to keep the guard from passing
-- vacuously.

DROP TABLE IF EXISTS t_nondet_left;
DROP TABLE IF EXISTS t_nondet_right;
CREATE TABLE t_nondet_left (k UInt32, a UInt8, b UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_nondet_right (k UInt32, c UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_nondet_left SELECT number, number % 2, number FROM numbers(100000);
INSERT INTO t_nondet_right SELECT number, 1 FROM numbers(100000);

-- 50000 rows pass on the `a = 1` branch, and the 50000 rows of the `a = 0` branch pass one coin
-- flip each, so the answer is ~75000 with a standard deviation of ~112. Evaluating the conjunct
-- twice would require two independent flips and give ~62500, far outside the accepted range.
SELECT count() BETWEEN 74000 AND 76000
FROM t_nondet_left AS t1 INNER JOIN t_nondet_right AS t2 ON t1.k = t2.k
WHERE (t1.a = 0 AND (t1.b + rand()) % 2 = 0) OR (t1.a = 1 AND t2.c >= 0)
SETTINGS use_join_disjunctions_push_down = 1;

SELECT count() BETWEEN 74000 AND 76000
FROM t_nondet_left AS t1 INNER JOIN t_nondet_right AS t2 ON t1.k = t2.k
WHERE (t1.a = 0 AND (t1.b + rand()) % 2 = 0) OR (t1.a = 1 AND t2.c >= 0)
SETTINGS use_join_disjunctions_push_down = 0;

SELECT 'a deterministic conjunct is still pushed down and keeps the same result';
SELECT count()
FROM t_nondet_left AS t1 INNER JOIN t_nondet_right AS t2 ON t1.k = t2.k
WHERE (t1.a = 0 AND t1.b < 1000) OR (t1.a = 1 AND t2.c >= 0)
SETTINGS use_join_disjunctions_push_down = 1;
SELECT count()
FROM t_nondet_left AS t1 INNER JOIN t_nondet_right AS t2 ON t1.k = t2.k
WHERE (t1.a = 0 AND t1.b < 1000) OR (t1.a = 1 AND t2.c >= 0)
SETTINGS use_join_disjunctions_push_down = 0;

DROP TABLE t_nondet_left;
DROP TABLE t_nondet_right;
