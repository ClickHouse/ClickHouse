-- Tags: long
-- Which `IN (subquery)` the in to join rewrite accepts, and that an accepted one gives the same
-- answer as the set it replaces.

SET enable_analyzer = 1;
SET allow_correlated_subqueries = 1;
SET rewrite_in_to_join = 1;

DROP TABLE IF EXISTS n;
DROP TABLE IF EXISTS s;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS w;

CREATE TABLE n (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE s (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t (id UInt64, b UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE w (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO n VALUES (1), (NULL);
INSERT INTO s VALUES (1), (2);
INSERT INTO t VALUES (1, 1, [1, 2]), (2, 3, [5]), (3, 4, [7]);
INSERT INTO w VALUES (1, 1), (2, 3), (3, 2);
SELECT '-- Left argument is a column';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id IN (SELECT k FROM s));

SELECT groupArray(c) FROM (SELECT id IN (SELECT k FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(c) FROM (SELECT id IN (SELECT k FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- Left argument is a column, and IN is negated';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id NOT IN (SELECT k FROM s));

SELECT groupArray(c) FROM (SELECT id NOT IN (SELECT k FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(c) FROM (SELECT id NOT IN (SELECT k FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- Left argument is an expression, and IN inside where';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id + 1 IN (SELECT k FROM s));

SELECT groupArray(c) FROM (SELECT id + 1 IN (SELECT k FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(c) FROM (SELECT id + 1 IN (SELECT k FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- Left argument is an Expression, and IN inside projection';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id + 1 IN (SELECT k FROM s) AS c FROM t WHERE b > 1);

SELECT groupArray(c) FROM (SELECT id + 1 IN (SELECT k FROM s) AS c FROM t WHERE b > 1 ORDER BY c) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(c) FROM (SELECT id + 1 IN (SELECT k FROM s) AS c FROM t WHERE b > 1 ORDER BY c) SETTINGS rewrite_in_to_join = 1;

SELECT '-- Left argument is a lambda';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE arrayExists(z -> z > id, arr) IN (SELECT k FROM s));

SELECT groupArray(c) FROM (SELECT arrayExists(z -> z > id, arr) IN (SELECT k FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(c) FROM (SELECT arrayExists(z -> z > id, arr) IN (SELECT k FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- Left argument is a tuple, and IN subquery returns multiple columns';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE (id, b) IN (SELECT k, v FROM w));

SELECT groupArray(c) FROM (SELECT (id, b) IN (SELECT k, v FROM w) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(c) FROM (SELECT (id, b) IN (SELECT k, v FROM w) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- Left argument is a grouping key';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id IN (SELECT k FROM s), count() FROM t GROUP BY id);

SELECT groupArray(c) FROM (SELECT id IN (SELECT k FROM s) AS c, count() FROM t GROUP BY id ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(c) FROM (SELECT id IN (SELECT k FROM s) AS c, count() FROM t GROUP BY id ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- The left argument is a grouping expression';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT b + 1 AS t1 FROM t GROUP BY t1 HAVING t1 IN (SELECT k FROM s));

SELECT groupArray(t1) FROM (SELECT b + 1 AS t1 FROM t GROUP BY t1 HAVING t1 IN (SELECT k FROM s) ORDER BY t1) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(t1) FROM (SELECT b + 1 AS t1 FROM t GROUP BY t1 HAVING t1 IN (SELECT k FROM s) ORDER BY t1) SETTINGS rewrite_in_to_join = 1;

SELECT '-- The subquery column has a different but comparable type';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id IN (SELECT toUInt16(k) FROM s));

SELECT groupArray(c) FROM (SELECT id IN (SELECT toUInt16(k) FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(c) FROM (SELECT id IN (SELECT toUInt16(k) FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT groupArray(c) FROM (SELECT id + 255 IN (SELECT toUInt8(k) FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(c) FROM (SELECT id + 255 IN (SELECT toUInt8(k) FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- The subquery column is Bool, which the key is cast to like the set casts it';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id - 1 IN (SELECT true) AS c FROM t);

SELECT groupArray(toString(c)) FROM (SELECT id - 1 IN (SELECT true) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(toString(c)) FROM (SELECT id - 1 IN (SELECT true) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT groupArray(toString(c)) FROM (SELECT k IN (SELECT true) AS c FROM n ORDER BY k) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(toString(c)) FROM (SELECT k IN (SELECT true) AS c FROM n ORDER BY k) SETTINGS rewrite_in_to_join = 1;

SELECT '-- `Array(UInt8)` against `Array(Bool)`, which a set casts without normalizing the elements';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT CAST([arr[1]], 'Array(UInt8)') IN (SELECT [true]) AS c FROM t);

SELECT groupArray(toString(c)) FROM (SELECT CAST([arr[1]], 'Array(UInt8)') IN (SELECT [true]) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(toString(c)) FROM (SELECT CAST([arr[1]], 'Array(UInt8)') IN (SELECT [true]) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- The subquery column carries the name of the key, which the join keys on by side';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id IN (SELECT k AS `__table1.id` FROM s));

SELECT count() FROM t WHERE id IN (SELECT k AS `__table1.id` FROM s) SETTINGS rewrite_in_to_join = 0;
SELECT count() FROM t WHERE id IN (SELECT k AS `__table1.id` FROM s) SETTINGS rewrite_in_to_join = 1;
SELECT count() FROM t WHERE id IN (SELECT k AS `__table1.id` FROM s) SETTINGS rewrite_in_to_join = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1;

SELECT '-- The same IN in the projection and in INTERPOLATE, which keeps its set';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id, b, (b IN (SELECT k FROM s)) AS f FROM t ORDER BY id WITH FILL FROM 1 TO 5 INTERPOLATE (b AS b + (b IN (SELECT k FROM s))));

SELECT groupArray((id, b, f)) FROM (SELECT id, b, (b IN (SELECT k FROM s)) AS f FROM t ORDER BY id WITH FILL FROM 1 TO 5 INTERPOLATE (b AS b + (b IN (SELECT k FROM s)))) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray((id, b, f)) FROM (SELECT id, b, (b IN (SELECT k FROM s)) AS f FROM t ORDER BY id WITH FILL FROM 1 TO 5 INTERPOLATE (b AS b + (b IN (SELECT k FROM s)))) SETTINGS rewrite_in_to_join = 1;

SELECT '-- Two columns of the subquery share a name';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE (id, b) IN (SELECT number AS `plus(number, 1)`, number + 1 FROM numbers(3)));

SELECT groupArray((id, b)) FROM (SELECT id, b FROM t WHERE (id, b) IN (SELECT number AS `plus(number, 1)`, number + 1 FROM numbers(3)) ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray((id, b)) FROM (SELECT id, b FROM t WHERE (id, b) IN (SELECT number AS `plus(number, 1)`, number + 1 FROM numbers(3)) ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- The subquery is a union';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id IN (SELECT k FROM s UNION ALL SELECT k FROM w));

SELECT groupArray(c) FROM (SELECT id IN (SELECT k FROM s UNION ALL SELECT k FROM w) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(c) FROM (SELECT id IN (SELECT k FROM s UNION ALL SELECT k FROM w) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- The result of IN is the only thing the projection reads, and there is a WHERE below it';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id IN (SELECT k FROM s) AS c FROM t WHERE b > 1);

SELECT groupArray(c) FROM (SELECT id IN (SELECT k FROM s) AS c FROM t WHERE b > 1 ORDER BY c) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(c) FROM (SELECT id IN (SELECT k FROM s) AS c FROM t WHERE b > 1 ORDER BY c) SETTINGS rewrite_in_to_join = 1;

SELECT '-- The projection reads the result of IN beside a column the WHERE step computed';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN WITH id > 1 AS f SELECT f, id IN (SELECT k FROM s) AS c FROM t WHERE f);

SELECT groupArray(concat(toString(f), toString(c))) FROM (WITH id > 1 AS f SELECT f, id IN (SELECT k FROM s) AS c FROM t WHERE f ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(concat(toString(f), toString(c))) FROM (WITH id > 1 AS f SELECT f, id IN (SELECT k FROM s) AS c FROM t WHERE f ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- The left argument has a scalar subquery folded to a constant';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id + (SELECT max(k) FROM s) IN (SELECT k FROM s));

SELECT groupArray(c) FROM (SELECT id + (SELECT max(k) FROM s) IN (SELECT k FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(c) FROM (SELECT id + (SELECT max(k) FROM s) IN (SELECT k FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- The left argument is nullable';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM n WHERE k IN (SELECT k FROM s));

SELECT groupArray(toString(c)) FROM (SELECT k IN (SELECT k FROM s) AS c FROM n ORDER BY k) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(toString(c)) FROM (SELECT k IN (SELECT k FROM s) AS c FROM n ORDER BY k) SETTINGS rewrite_in_to_join = 1;

SELECT '-- The left argument is nullable, and IN is negated';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM n WHERE k NOT IN (SELECT k FROM s));

SELECT count() FROM n WHERE k NOT IN (SELECT k FROM s) SETTINGS rewrite_in_to_join = 0;
SELECT count() FROM n WHERE k NOT IN (SELECT k FROM s) SETTINGS rewrite_in_to_join = 1;

SELECT '-- The left argument is a window function, whose result the window step already produced';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT row_number() OVER (ORDER BY id) IN (SELECT k FROM s) FROM t);

SELECT groupArray(toString(c)) FROM (SELECT row_number() OVER (ORDER BY id) IN (SELECT k FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(toString(c)) FROM (SELECT row_number() OVER (ORDER BY id) IN (SELECT k FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- The same IN in two clauses of one query builds one join';
SELECT countIf(explain LIKE '%JoinLogical%'), countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN keep_logical_steps = 1 SELECT id NOT IN (SELECT k FROM s) AS c FROM t WHERE (id NOT IN (SELECT k FROM s)) OR b > 3);

SELECT groupArray(concat(toString(id), toString(c))) FROM (SELECT id, id NOT IN (SELECT k FROM s) AS c FROM t WHERE (id NOT IN (SELECT k FROM s)) OR b > 3 ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(concat(toString(id), toString(c))) FROM (SELECT id, id NOT IN (SELECT k FROM s) AS c FROM t WHERE (id NOT IN (SELECT k FROM s)) OR b > 3 ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- The same IN in two clauses, spelled with a WITH alias';
SELECT countIf(explain LIKE '%JoinLogical%'), countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN keep_logical_steps = 1 WITH id NOT IN (SELECT k FROM s) AS c SELECT id, c FROM t WHERE c OR b > 3);

SELECT groupArray(concat(toString(id), toString(c))) FROM (WITH id NOT IN (SELECT k FROM s) AS c SELECT id, c FROM t WHERE c OR b > 3 ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(concat(toString(id), toString(c))) FROM (WITH id NOT IN (SELECT k FROM s) AS c SELECT id, c FROM t WHERE c OR b > 3 ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- ORDER BY ALL over a projection that reads the result of IN';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id IN (SELECT k FROM s) AS a FROM t ORDER BY ALL);

SELECT groupArray(toString(a)) FROM (SELECT id IN (SELECT k FROM s) AS a FROM t ORDER BY ALL) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(toString(a)) FROM (SELECT id IN (SELECT k FROM s) AS a FROM t ORDER BY ALL) SETTINGS rewrite_in_to_join = 1;

SELECT '-- An `arrayJoin` in the left argument is evaluated once, below the join';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE arrayJoin(arr) IN (SELECT k FROM s));

SELECT groupArray(z) FROM (SELECT arrayJoin(arr) AS z FROM t WHERE z IN (SELECT k FROM s) ORDER BY z) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(z) FROM (SELECT arrayJoin(arr) AS z FROM t WHERE z IN (SELECT k FROM s) ORDER BY z) SETTINGS rewrite_in_to_join = 1;

SELECT groupArray(concat(toString(z), toString(c))) FROM (SELECT arrayJoin(arr) AS z, z IN (SELECT k FROM s) AS c FROM t ORDER BY z) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(concat(toString(z), toString(c))) FROM (SELECT arrayJoin(arr) AS z, z IN (SELECT k FROM s) AS c FROM t ORDER BY z) SETTINGS rewrite_in_to_join = 1;

SELECT '-- A non-deterministic left argument is evaluated once, below the join';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM numbers(10) WHERE rand() % 2 IN (SELECT 1));

SELECT countIf(r != c) FROM (SELECT rand() % 2 AS r, r IN (SELECT 1) AS c FROM numbers(200000)) SETTINGS rewrite_in_to_join = 0;
SELECT countIf(r != c) FROM (SELECT rand() % 2 AS r, r IN (SELECT 1) AS c FROM numbers(200000)) SETTINGS rewrite_in_to_join = 1;

SELECT '-- A set size limit applies to the join the rewrite builds';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id IN (SELECT k FROM s) SETTINGS max_rows_in_set = 1);

SELECT count() FROM t WHERE id IN (SELECT k FROM s) SETTINGS max_rows_in_set = 1, rewrite_in_to_join = 0; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM t WHERE id IN (SELECT k FROM s) SETTINGS max_rows_in_set = 1, rewrite_in_to_join = 1; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM t WHERE id IN (SELECT k FROM s) SETTINGS max_bytes_in_set = 1, rewrite_in_to_join = 0; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM t WHERE id IN (SELECT k FROM s) SETTINGS max_bytes_in_set = 1, rewrite_in_to_join = 1; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT '-- IN subquery inside HAVING';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id FROM t GROUP BY id HAVING id IN (SELECT k FROM s));

SELECT groupArray(id) FROM (SELECT id FROM t GROUP BY id HAVING id IN (SELECT k FROM s) ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(id) FROM (SELECT id FROM t GROUP BY id HAVING id IN (SELECT k FROM s) ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- IN subquery inside QUALIFY';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id, row_number() OVER (ORDER BY id) AS r FROM t QUALIFY id IN (SELECT k FROM s));

SELECT groupArray(id) FROM (SELECT id, row_number() OVER (ORDER BY id) AS r FROM t QUALIFY id IN (SELECT k FROM s) ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(id) FROM (SELECT id, row_number() OVER (ORDER BY id) AS r FROM t QUALIFY id IN (SELECT k FROM s) ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- IN subquery as a grouping key';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id IN (SELECT k FROM s) AS g, count() FROM t GROUP BY g);

SELECT groupArray((g, n)) FROM (SELECT id IN (SELECT k FROM s) AS g, count() AS n FROM t GROUP BY g ORDER BY g) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray((g, n)) FROM (SELECT id IN (SELECT k FROM s) AS g, count() AS n FROM t GROUP BY g ORDER BY g) SETTINGS rewrite_in_to_join = 1;

SELECT '-- IN subquery as an argument of an aggregate function';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT sum(id IN (SELECT k FROM s)) FROM t);

SELECT sum(id IN (SELECT k FROM s)) FROM t SETTINGS rewrite_in_to_join = 0;
SELECT sum(id IN (SELECT k FROM s)) FROM t SETTINGS rewrite_in_to_join = 1;

SELECT '-- The left argument is an aggregate function';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT sum(b) AS x FROM t GROUP BY id HAVING x IN (SELECT k FROM s));

SELECT groupArray(x) FROM (SELECT sum(b) AS x FROM t GROUP BY id HAVING x IN (SELECT k FROM s) ORDER BY x) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(x) FROM (SELECT sum(b) AS x FROM t GROUP BY id HAVING x IN (SELECT k FROM s) ORDER BY x) SETTINGS rewrite_in_to_join = 1;

SELECT '-- IN subquery inside a window function';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id, row_number() OVER (ORDER BY id IN (SELECT k FROM s), id) AS r FROM t);

SELECT groupArray((id, r)) FROM (SELECT id, row_number() OVER (ORDER BY id IN (SELECT k FROM s), id) AS r FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray((id, r)) FROM (SELECT id, row_number() OVER (ORDER BY id IN (SELECT k FROM s), id) AS r FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- IN subquery inside ORDER BY';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id FROM t ORDER BY id IN (SELECT k FROM s), id);

SELECT groupArray(id) FROM (SELECT id FROM t ORDER BY id IN (SELECT k FROM s), id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(id) FROM (SELECT id FROM t ORDER BY id IN (SELECT k FROM s), id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- IN subquery inside LIMIT BY';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id FROM t ORDER BY id LIMIT 1 BY id IN (SELECT k FROM s));

SELECT groupArray(id) FROM (SELECT id FROM t ORDER BY id LIMIT 1 BY id IN (SELECT k FROM s)) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(id) FROM (SELECT id FROM t ORDER BY id LIMIT 1 BY id IN (SELECT k FROM s)) SETTINGS rewrite_in_to_join = 1;

SELECT '-- A correlated subquery inside the left argument';
SELECT countIf(explain LIKE '%JoinLogical%'), countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN keep_logical_steps = 1 SELECT count() FROM t WHERE (1 + (SELECT max(v) FROM w WHERE w.k = t.id)) IN (SELECT k FROM s));

SELECT count() FROM t WHERE (1 + (SELECT max(v) FROM w WHERE w.k = t.id)) IN (SELECT k FROM s) SETTINGS rewrite_in_to_join = 0;
SELECT count() FROM t WHERE (1 + (SELECT max(v) FROM w WHERE w.k = t.id)) IN (SELECT k FROM s) SETTINGS rewrite_in_to_join = 1;

SELECT '-- An IN inside the left argument of another IN builds one join for each';
SELECT countIf(explain LIKE '%JoinLogical%'), countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN keep_logical_steps = 1 SELECT count() FROM t WHERE (id IN (SELECT k FROM s)) IN (SELECT toUInt8(1)));

SELECT count() FROM t WHERE (id IN (SELECT k FROM s)) IN (SELECT toUInt8(1)) SETTINGS rewrite_in_to_join = 0;
SELECT count() FROM t WHERE (id IN (SELECT k FROM s)) IN (SELECT toUInt8(1)) SETTINGS rewrite_in_to_join = 1;

SELECT '-- Query with IN subquery and another correlated subquery';
SELECT countIf(explain LIKE '%Join%') > 1
FROM (EXPLAIN SELECT count() FROM t WHERE id IN (SELECT k FROM s) AND b >= (SELECT max(k) FROM w WHERE w.v = t.id));

SELECT count() FROM t WHERE id IN (SELECT k FROM s) AND b >= (SELECT max(k) FROM w WHERE w.v = t.id) SETTINGS rewrite_in_to_join = 0;
SELECT count() FROM t WHERE id IN (SELECT k FROM s) AND b >= (SELECT max(k) FROM w WHERE w.v = t.id) SETTINGS rewrite_in_to_join = 1;

DROP TABLE t;
DROP TABLE s;
DROP TABLE w;
DROP TABLE n;
