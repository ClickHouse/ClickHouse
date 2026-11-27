-- Tags: no-old-analyzer

-- Port of DuckDB test/sql/join/iejoin/test_iejoin_predicate.test (previously skipped: every
-- query needs a third arbitrary predicate). One-second bands make each row overlap only itself,
-- and the tail predicate `l.k100 + r.k100 < 10` keeps ids 0..4: INNER emits the diagonal,
-- the outer kinds pad the ids the tail filtered out. `join_use_nulls` matches DuckDB's padding.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET join_use_nulls = 1;

DROP TABLE IF EXISTS tleft;

CREATE TABLE tleft ENGINE = MergeTree ORDER BY tuple() AS
SELECT number AS k100,
       toDateTime('2024-01-01 00:00:00', 'UTC') + toIntervalSecond(number) AS b100,
       b100 + toIntervalSecond(1) AS e100
FROM numbers(10);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM tleft l JOIN tleft r ON l.b100 < r.e100 AND r.b100 < l.e100 AND l.k100 + r.k100 < 10) WHERE explain LIKE '%IEJoin%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM tleft l LEFT JOIN tleft r ON l.b100 < r.e100 AND r.b100 < l.e100 AND l.k100 + r.k100 < 10) WHERE explain LIKE '%IEJoin%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM tleft l LEFT JOIN tleft r ON l.b100 < r.e100 AND r.b100 < l.e100 AND l.k100 + r.k100 < 10) WHERE explain LIKE '%Residual filter%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM tleft l RIGHT JOIN tleft r ON l.b100 < r.e100 AND r.b100 < l.e100 AND l.k100 + r.k100 < 10) WHERE explain LIKE '%IEJoin%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM tleft l FULL JOIN tleft r ON l.b100 < r.e100 AND r.b100 < l.e100 AND l.k100 + r.k100 < 10) WHERE explain LIKE '%IEJoin%';

SELECT 'inner';
SELECT l.k100 AS lid, r.k100 AS rid
FROM tleft l INNER JOIN tleft r ON l.b100 < r.e100 AND r.b100 < l.e100 AND l.k100 + r.k100 < 10
ORDER BY 1;

SELECT 'left';
SELECT l.k100 AS lid, r.k100 AS rid
FROM tleft l LEFT JOIN tleft r ON l.b100 < r.e100 AND r.b100 < l.e100 AND l.k100 + r.k100 < 10
ORDER BY 1;

SELECT 'right';
SELECT l.k100 AS lid, r.k100 AS rid
FROM tleft l RIGHT JOIN tleft r ON l.b100 < r.e100 AND r.b100 < l.e100 AND l.k100 + r.k100 < 10
ORDER BY 2;

SELECT 'full';
SELECT l.k100 AS lid, r.k100 AS rid
FROM tleft l FULL JOIN tleft r ON l.b100 < r.e100 AND r.b100 < l.e100 AND l.k100 + r.k100 < 10
ORDER BY ALL;

DROP TABLE tleft;
