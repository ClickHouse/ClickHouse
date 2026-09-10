-- Conformance queries derived from the Trino test suite
-- (https://github.com/trinodb/trino, core/trino-main/src/test/java/io/trino/sql/query, Apache License 2.0).
-- Expected results verified against the assertions of the original tests.

SET allow_experimental_trino_dialect = 1;
SET dialect = 'trino';

SELECT '-- TestAggregation';
SELECT count_if(v > ALL (VALUES 0, 1)) FROM (VALUES (1, 1), (1, 2)) t(k, v) GROUP BY k;
SELECT count_if(v > ANY (VALUES 0, 1)) FROM (VALUES (1, 1), (1, 2)) t(k, v) GROUP BY k;
SELECT count(DISTINCT 'x'), count(*) FROM (VALUES 1, 2, 3);

SELECT '-- TestArraySortAfterArrayDistinct';
SELECT ARRAY_DISTINCT(ARRAY_SORT(items)) as result from (VALUES (ARRAY ['elephant', 'dog', 'cat', 'dog'])) t(items);

SELECT '-- TestCorrelatedAggregation';
SELECT * FROM (SELECT key, BOOL_OR(value) AS bool_or_value FROM (VALUES (2, null), (3, false), (4, true)) t2(key, value) GROUP BY key) WHERE bool_or_value = true;

SELECT '-- TestDistinctAggregations';
SELECT count(DISTINCT x) FROM (VALUES 1, 1, 2, 3) t(x);
SELECT count(DISTINCT x), sum(DISTINCT x) FROM (VALUES 1, 1, 2, 3) t(x);
SELECT count(DISTINCT x), count(*) FROM (VALUES 1, 1, 2, 3) t(x);
SELECT count(DISTINCT x), count(DISTINCT y) FROM (VALUES (1, 10), (1, 20), (1, 30), (2, 30)) t(x, y);
SELECT corr(DISTINCT x, y) FROM (VALUES (1, 1), (2, 2), (2, 2), (3, 3)) t(x, y);
SELECT corr(DISTINCT x, y), corr(DISTINCT y, x) FROM (VALUES (1, 1), (2, 2), (2, 2), (3, 3)) t(x, y);

SELECT '-- TestFilteredAggregations';
SELECT sum(x) FILTER(WHERE x > 0) FROM (VALUES 1, 1, 0, 2, 3, 3) t(x);
SELECT sum(x) FILTER(WHERE x > 0), sum(x) FILTER(WHERE x < 3) FROM (VALUES 1, 1, 0, 5, 3, 8) t(x);
SELECT sum(x) FILTER(WHERE x > 1), sum(x) FROM (VALUES 1, 1, 0, 2, 3, 3) t(x);
SELECT count(DISTINCT x) FILTER (WHERE x > 1) FROM (VALUES 1, 1, 1, 2, 3, 3) t(x);
SELECT count(DISTINCT x) FILTER (WHERE x > 1), sum(DISTINCT x) FROM (VALUES 1, 1, 1, 2, 3, 3) t(x);
SELECT count(DISTINCT x) FILTER (WHERE x > 1), sum(DISTINCT y) FILTER (WHERE x < 3)FROM (VALUES (1, 10),(1, 20),(1, 20),(2, 20),(3, 30)) t(x, y);

SELECT '-- TestFormat';
SELECT format('%.6f', sum(1000000 / 1e6));
SELECT format('%.6f', avg(1));
SELECT format('%d', count(1));
SELECT format('%d', arbitrary(1));
SELECT format('%d', cast(sum(totalprice) as bigint)) FROM (VALUES 20,99,15) t(totalprice);

SELECT '-- TestGroupBy';
SELECT CAST(row(x) AS row("A" bigint)) FROM (VALUES 42) t(x) GROUP BY CAST(row(x) AS row("A" bigint));
SELECT a + 1, a + 1 FROM (VALUES 1) t(a) GROUP BY 1, 2;
SELECT 1 FROM (VALUES 1) t(a) GROUP BY a + 1, a + 1;
SELECT 1 FROM (VALUES 1) t(a) GROUP BY t.a + 1, a + 1;
SELECT a + 1 FROM (VALUES 1) t(a) GROUP BY t.a + 1;
SELECT t.a + 1 FROM (VALUES 1) t(a) GROUP BY a + 1;

SELECT '-- TestGrouping';
SELECT GROUPING(k), SUM(v) + 1e0 FROM (VALUES (1, 1)) AS t(k,v) GROUP BY k;

SELECT '-- TestJoin';
WITH a AS (SELECT id FROM (VALUES (1)) AS t(id)), b AS (SELECT id FROM (VALUES (1)) AS t(id)), c AS (SELECT id FROM (VALUES ('1')) AS t(id)), d as (SELECT id FROM (VALUES (1)) AS t(id)) SELECT a.id FROM a LEFT JOIN b ON a.id = b.id JOIN c ON a.id = CAST(c.id AS bigint) JOIN d ON d.id = a.id;
WITH t(x) AS (VALUES nan()) SELECT * FROM t t1 JOIN t t2 ON NOT t1.x < t2.x;
WITH t1 (id, v) as ( VALUES (1, 100), (2, 200)), t2 (id, x, y) AS ( VALUES (1, 10, 'a'), (2, 10, 'b')) SELECT x, y FROM t1 JOIN t2 ON (t1.id = t2.id) WHERE IF(t1.v = 0, 'cc', y) = 'b';
SELECT 5 FROM (VALUES (1,'foo')) l(l1, l2) LEFT JOIN (VALUES (2,'bar')) r(r1, r2) ON l2 = r2 WHERE l1 >= COALESCE(r1, 0);
SELECT 5 FROM (VALUES (2,'foo')) l(l1, l2) RIGHT JOIN (VALUES (1,'bar')) r(r1, r2) ON l2 = r2 WHERE r1 >= COALESCE(l1, 0);
WITH t(x,y) AS ( VALUES ('a', '1'), ('b', 'x'), (null, 'y') ), u(x,y) AS ( VALUES ('a', '1'), ('c', 'x'), (null, 'y') ) SELECT * FROM t JOIN u ON t.x = u.x WHERE CAST(t.y AS int) = 1;

SELECT '-- TestJoinUsing';
SELECT k, v1, v2, t.v1, u.v2 FROM (VALUES (1, 'a')) AS t(k, v1) JOIN(VALUES (1, 'b')) AS u(k, v2) USING (k);
SELECT * FROM (VALUES (1, 'a')) AS t(k, v1) JOIN(VALUES (1, 'b')) AS u(k, v2) USING (k);
SELECT * FROM (VALUES (1, 'a', 2)) AS t(k1, v1, k2) JOIN(VALUES (1, 'b', 2)) AS u(k1, v2, k2) USING (k1, k2);
SELECT * FROM (VALUES (1e0, 'a')) AS t(k, v1) JOIN(VALUES (1, 'b')) AS u(k, v2) USING (k);
SELECT * FROM (VALUES (1, 2e0)) x (a, b) JOIN (VALUES (DOUBLE '1.0', 3)) y (a, b) USING(a);
SELECT * FROM (VALUES (1.0E0, 2e0)) x (a, b) JOIN (VALUES (1, 3)) y (a, b) USING(a);

SELECT '-- TestLambdaExpressions';
SELECT cardinality(filter(a, x -> x > 0)) FROM (VALUES ARRAY[1,2,3], ARRAY[0,1,2], ARRAY[0,0,0]) AS t(a) GROUP BY cardinality(filter(a, x -> x > 0))ORDER BY cardinality(filter(a, x -> x > 0));
SELECT transform(a, x -> x + 1), transform(b, x -> x + 1) FROM (VALUES ROW(ARRAY[1, 2, 3], ARRAY[10, 20, 30])) t(a, b);
SELECT transform(a, x -> x + 1), transform(b, x -> x + 1) FROM (VALUES ROW(ARRAY[1, 2, 3], ARRAY[10e0, 20e0, 30e0])) t(a, b);
SELECT transform(a, x -> transform(ARRAY[x], x -> x + 1)) FROM (VALUES ARRAY[1, 2, 3]) t(a);
SELECT transform(a, x -> transform(ARRAY[x], y -> y + 1)) FROM (VALUES ARRAY[1, 2, 3]) t(a);

SELECT '-- TestListagg';
SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value) FROM (VALUES 'a') t(value);
SELECT id, listagg(value, ',') WITHIN GROUP (ORDER BY value) FROM (VALUES (1, 'a')) t(id, value) GROUP BY id ORDER BY id;
SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value) FROM (VALUES 'a', 'bb', 'ccc', 'dddd') t(value);
SELECT listagg(value) WITHIN GROUP (ORDER BY value) FROM (VALUES 'a', 'b', 'c') t(value);
SELECT id, listagg(value) WITHIN GROUP (ORDER BY value) FROM (VALUES (1, 'c'), (2, 'b'), (1, 'a'), (2, 'd') ) t(id, value) GROUP BY id ORDER BY id;
SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value DESC) FROM (VALUES 'a', 'bb', 'ccc', 'dddd') t(value);

SELECT '-- TestOrderedAggregation';
SELECT sum(x ORDER BY y) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y);
SELECT array_agg(x ORDER BY y) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y);
SELECT array_agg(x ORDER BY y DESC) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y);
SELECT array_agg(x ORDER BY x DESC) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y);
SELECT array_agg(x ORDER BY x) FROM (VALUES ('a', 2), ('bcd', 5), ('abcd', 1)) t(x, y);
SELECT array_agg(y ORDER BY x) FROM (VALUES ('a', 2), ('bcd', 5), ('abcd', 1)) t(x, y);

SELECT '-- TestSelectAll';
SELECT (ROW (1, 'a')).*;
SELECT (1, 2).*, 3;
SELECT 1, (2, 3).*;
SELECT a, b, c FROM (SELECT T.* FROM (VALUES (1, 2, 3)) T (a, b, c));
SELECT r, x, count(x), t.r.* FROM (VALUES (ROW(1), 'a'), (ROW(2), 'b'), (ROW(1), 'a'), (ROW(1), 'b')) t(r, x) GROUP BY r, x ORDER BY r, x DESC;
SELECT array_agg(x), t.r.* FROM (VALUES (ROW(1), 'a'), (ROW(2), 'b'), (ROW(1), 'a'), (ROW(1), 'b')) t(r, x) GROUP BY r, x ORDER BY r, x DESC;

SELECT '-- TestSubqueries';
SELECT((SELECT c FROM (SELECT b FROM (VALUES (1, 2), (1, 2)) inner_relation(a, b) WHERE a = 1 LIMIT 1) C(c) WHERE c = d)) FROM (VALUES 2) D(d);
SELECT * FROM (VALUES 1, 2) t2(b) WHERE (SELECT b) = 2;
SELECT 1 FROM (VALUES 1, 2) t1(b) WHERE 1 = (SELECT cast(b as decimal(7,2)));
SELECT (SELECT outer_relation.b FROM (VALUES 1) inner_relation) FROM (values 2) outer_relation(b);
SELECT (SELECT a + b FROM (VALUES 1) inner_relation(a)) FROM (VALUES 2) outer_relation(b);
SELECT (SELECT array_agg(x) FROM UNNEST(a) u(x)) FROM (VALUES ARRAY[1, 2, 3]) t(a);

SELECT '-- TestValues';
VALUES (1, 'a');
VALUES CAST(ROW(1, 'TruE') AS row(double, boolean));
VALUES null;
VALUES (null, null);

SELECT '-- TestWindow';
SELECT SUM(a) OVER w, COUNT(a) OVER w, MIN(a) OVER w, MAX(a) OVER w, SUM(b) OVER w, COUNT(b) OVER w, MIN(b) OVER w, MAX(b) OVER w, SUM(c) OVER w, COUNT(c) OVER w, MIN(c) OVER w, MAX(c) OVER w, SUM(d) OVER w, COUNT(d) OVER w, MIN(d) OVER w, MAX(d) OVER w, SUM(e) OVER w, COUNT(e) OVER w, MIN(e) OVER w, MAX(e) OVER w, SUM(f) OVER w, COUNT(f) OVER w, MIN(f) OVER w, MAX(f) OVER w FROM ( VALUES (1, 1, 1, 1, 1, 1, 1) ) AS t(k, a, b, c, d, e, f) WINDOW w AS (ORDER BY k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
