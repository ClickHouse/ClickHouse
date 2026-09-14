-- Tests which `IN (subquery)` the in to join rewrite accepts and which it declines, and that an
-- accepted one gives the same answer as the set.

SET enable_analyzer = 1;
SET allow_correlated_subqueries = 1;
SET rewrite_in_to_join = 1;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS s;
DROP TABLE IF EXISTS w;
DROP TABLE IF EXISTS n;

CREATE TABLE t (id UInt64, b UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE s (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE w (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE n (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t VALUES (1, 1, [1, 2]), (2, 3, [5]), (3, 4, [7]);
INSERT INTO s VALUES (1), (2);
INSERT INTO w VALUES (1, 1), (2, 3), (3, 2);
INSERT INTO n VALUES (1), (NULL);

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

SELECT '-- Left argument is an expression';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id + 1 IN (SELECT k FROM s));

SELECT groupArray(c) FROM (SELECT id + 1 IN (SELECT k FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(c) FROM (SELECT id + 1 IN (SELECT k FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

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

SELECT '-- The subquery column has a different but comparable type';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id IN (SELECT toUInt16(k) FROM s));

SELECT groupArray(c) FROM (SELECT id IN (SELECT toUInt16(k) FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(c) FROM (SELECT id IN (SELECT toUInt16(k) FROM s) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- The subquery is a union';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id IN (SELECT k FROM s UNION ALL SELECT k FROM w));

SELECT groupArray(c) FROM (SELECT id IN (SELECT k FROM s UNION ALL SELECT k FROM w) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(c) FROM (SELECT id IN (SELECT k FROM s UNION ALL SELECT k FROM w) AS c FROM t ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- IN as a grouping key is not rewritten';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id IN (SELECT k FROM s) AS g, count() FROM t GROUP BY g);

SELECT '-- IN with a subquery in the left argument is not rewritten';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id + (SELECT max(k) FROM s) IN (SELECT k FROM s));

SELECT '-- IN with a nullable left argument is not rewritten';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM n WHERE k IN (SELECT k FROM s));

SELECT '-- IN with a constant left argument is not rewritten';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE 1 IN (SELECT k FROM s));

SELECT '-- IN inside PREWHERE is not rewritten';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t PREWHERE id IN (SELECT k FROM s));

SELECT '-- IN with an incomparable subquery column type is not rewritten';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id IN (SELECT toInt8(k) FROM s));

SELECT '-- IN inside a lambda is not rewritten';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE arrayExists(z -> z IN (SELECT k FROM s), arr));

SELECT '-- IN as an argument of an aggregate function is not rewritten';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT sum(id IN (SELECT k FROM s)) FROM t);

SELECT '-- `nullIn` is not rewritten';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id IN (SELECT k FROM s) SETTINGS transform_null_in = 1);

SELECT '-- IN suquery inside HAVING';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id FROM t GROUP BY id HAVING id IN (SELECT k FROM s));

SELECT groupArray(id) FROM (SELECT id FROM t GROUP BY id HAVING id IN (SELECT k FROM s) ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(id) FROM (SELECT id FROM t GROUP BY id HAVING id IN (SELECT k FROM s) ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- IN suquery inside QUALIFY';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id, row_number() OVER (ORDER BY id) AS r FROM t QUALIFY id IN (SELECT k FROM s));

SELECT groupArray(id) FROM (SELECT id, row_number() OVER (ORDER BY id) AS r FROM t QUALIFY id IN (SELECT k FROM s) ORDER BY id) SETTINGS rewrite_in_to_join = 0;
SELECT groupArray(id) FROM (SELECT id, row_number() OVER (ORDER BY id) AS r FROM t QUALIFY id IN (SELECT k FROM s) ORDER BY id) SETTINGS rewrite_in_to_join = 1;

SELECT '-- Query with IN subquery and another correlated subquery';
SELECT countIf(explain LIKE '%Join%') > 1
FROM (EXPLAIN SELECT count() FROM t WHERE id IN (SELECT k FROM s) AND b >= (SELECT max(k) FROM w WHERE w.v = t.id));

SELECT count() FROM t WHERE id IN (SELECT k FROM s) AND b >= (SELECT max(k) FROM w WHERE w.v = t.id) SETTINGS rewrite_in_to_join = 0;
SELECT count() FROM t WHERE id IN (SELECT k FROM s) AND b >= (SELECT max(k) FROM w WHERE w.v = t.id) SETTINGS rewrite_in_to_join = 1;

DROP TABLE t;
DROP TABLE s;
DROP TABLE w;
DROP TABLE n;
