-- https://github.com/ClickHouse/ClickHouse/issues/116907
-- `optimize_uniq_to_count` rewrites `uniq(x)` over a `SELECT DISTINCT`/`GROUP BY` subquery into
-- `count`. That assumes one emitted row per distinct value, and that every emitted row counts.
-- A NULL breaks the second assumption (`uniq` skips it, an argument-less `count` does not) and
-- `WITH ROLLUP`/`WITH CUBE` break the first (they emit extra super-aggregate rows).

SET optimize_uniq_to_count = 1;

SELECT 'nullable distinct';
SELECT uniq(x) FROM (SELECT DISTINCT x FROM values('x Nullable(Int64)', (1), (NULL), (2)));
SELECT uniq(x) FROM (SELECT DISTINCT x FROM values('x Nullable(Int64)', (1), (NULL), (2))) SETTINGS optimize_uniq_to_count = 0;
SELECT uniqExact(x) FROM (SELECT DISTINCT x FROM values('x Nullable(Int64)', (1), (NULL), (2)));
SELECT uniqExact(x) FROM (SELECT DISTINCT x FROM values('x Nullable(Int64)', (1), (NULL), (2))) SETTINGS optimize_uniq_to_count = 0;

SELECT 'nullable group by';
SELECT uniq(x) FROM (SELECT x FROM values('x Nullable(Int64)', (1), (NULL), (2)) GROUP BY x);
SELECT uniq(x) FROM (SELECT x FROM values('x Nullable(Int64)', (1), (NULL), (2)) GROUP BY x) SETTINGS optimize_uniq_to_count = 0;

SELECT 'low cardinality nullable';
SELECT uniq(x) FROM (SELECT DISTINCT x FROM (SELECT CAST(number % 3 = 0 ? NULL : toString(number), 'LowCardinality(Nullable(String))') AS x FROM numbers(6)));
SELECT uniq(x) FROM (SELECT DISTINCT x FROM (SELECT CAST(number % 3 = 0 ? NULL : toString(number), 'LowCardinality(Nullable(String))') AS x FROM numbers(6))) SETTINGS optimize_uniq_to_count = 0;

SELECT 'two nullable arguments';
SELECT uniq(x, y) FROM (SELECT DISTINCT x, y FROM values('x Nullable(Int64), y Int64', (1, 1), (NULL, 2), (2, 3)));
SELECT uniq(x, y) FROM (SELECT DISTINCT x, y FROM values('x Nullable(Int64), y Int64', (1, 1), (NULL, 2), (2, 3))) SETTINGS optimize_uniq_to_count = 0;

SELECT 'rollup';
SELECT uniq(x) FROM (SELECT x FROM values('x Int64', (0), (1), (1)) GROUP BY x WITH ROLLUP);
SELECT uniq(x) FROM (SELECT x FROM values('x Int64', (0), (1), (1)) GROUP BY x WITH ROLLUP) SETTINGS optimize_uniq_to_count = 0;

SELECT 'cube';
SELECT uniq(x) FROM (SELECT x FROM values('x Int64', (0), (1), (1)) GROUP BY x WITH CUBE);
SELECT uniq(x) FROM (SELECT x FROM values('x Int64', (0), (1), (1)) GROUP BY x WITH CUBE) SETTINGS optimize_uniq_to_count = 0;

-- The same shapes on the legacy `RewriteUniqToCountVisitor` path.
SELECT 'legacy analyzer';
SET enable_analyzer = 0;
SELECT uniq(x) FROM (SELECT DISTINCT x FROM values('x Nullable(Int64)', (1), (NULL), (2)));
SELECT uniq(x) FROM (SELECT DISTINCT x FROM values('x Nullable(Int64)', (1), (NULL), (2))) SETTINGS optimize_uniq_to_count = 0;
SELECT uniq(x) FROM (SELECT x FROM values('x Nullable(Int64)', (1), (NULL), (2)) GROUP BY x);
SELECT uniq(x) FROM (SELECT x FROM values('x Nullable(Int64)', (1), (NULL), (2)) GROUP BY x) SETTINGS optimize_uniq_to_count = 0;
SELECT uniq(x, y) FROM (SELECT DISTINCT x, y FROM values('x Nullable(Int64), y Int64', (1, 1), (NULL, 2), (2, 3)));
SELECT uniq(x, y) FROM (SELECT DISTINCT x, y FROM values('x Nullable(Int64), y Int64', (1, 1), (NULL, 2), (2, 3))) SETTINGS optimize_uniq_to_count = 0;
SELECT uniq(x) FROM (SELECT x FROM values('x Int64', (0), (1), (1)) GROUP BY x WITH ROLLUP);
SELECT uniq(x) FROM (SELECT x FROM values('x Int64', (0), (1), (1)) GROUP BY x WITH ROLLUP) SETTINGS optimize_uniq_to_count = 0;
SELECT uniq(x) FROM (SELECT x FROM values('x Int64', (0), (1), (1)) GROUP BY x WITH CUBE);
SELECT uniq(x) FROM (SELECT x FROM values('x Int64', (0), (1), (1)) GROUP BY x WITH CUBE) SETTINGS optimize_uniq_to_count = 0;
-- The legacy rewrite fires, and it produces `count(x)`, not an argument-less `count()`.
SELECT count() > 0 FROM (EXPLAIN SYNTAX SELECT uniq(x) FROM (SELECT DISTINCT x FROM values('x Nullable(Int64)', (1), (NULL), (2)))) WHERE explain LIKE '%count(x)%';
SET enable_analyzer = 1;

-- The rewrite still fires, both for a non-Nullable and for a Nullable column without modifiers.
SELECT 'still optimized';
SELECT uniq(x) FROM (SELECT DISTINCT x FROM values('x Int64', (1), (1), (2)));
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT uniq(x) FROM (SELECT DISTINCT x FROM values('x Int64', (1), (1), (2)))) WHERE explain LIKE '%function_name: count%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT uniq(x) FROM (SELECT DISTINCT x FROM values('x Nullable(Int64)', (1), (NULL), (2)))) WHERE explain LIKE '%function_name: count%';
