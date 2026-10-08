-- A number literal in arithmetic with a `Decimal` column is typed the same way in a query and in the key,
-- skip index and projection expressions of a table, so they are used for the same expression in a query.
-- A comparison with a `Decimal` column is exact, and it is used with a key on that column.

DROP TABLE IF EXISTS t_key;
DROP TABLE IF EXISTS t_projection;
DROP TABLE IF EXISTS t_price;

CREATE TABLE t_key (price Decimal64(2), INDEX idx price / 3.0 TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY toInt64(price * 100.0) SETTINGS index_granularity = 2;
INSERT INTO t_key SELECT number FROM numbers(20);
SELECT 'primary key', extract(explain, '(Name: idx|Granules: \d+/\d+)') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key WHERE toInt64(price * 100.0) = 500)
WHERE explain LIKE '%Granules: %/%' LIMIT 1;
SELECT 'skip index', extract(explain, '(Name: idx|Granules: \d+/\d+)') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key WHERE price / 3.0 > 5)
WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules: %/%';

CREATE TABLE t_projection (price Decimal64(2), PROJECTION p (SELECT toInt64(price * 100.0) AS c, count() GROUP BY c))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_projection SELECT number FROM numbers(20);
SELECT 'projection', count() FROM (SELECT toInt64(price * 100.0) AS c, count() FROM t_projection GROUP BY c)
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

CREATE TABLE t_price (price Decimal64(2)) ENGINE = MergeTree ORDER BY price SETTINGS index_granularity = 2;
INSERT INTO t_price SELECT number FROM numbers(40);
SELECT 'divide', extract(explain, '(Name: idx|Granules: \d+/\d+)') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_price WHERE price / 3.0 > 5)
WHERE explain LIKE '%Granules: %/%' LIMIT 1;
SELECT 'intDiv', extract(explain, '(Name: idx|Granules: \d+/\d+)') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_price WHERE intDiv(price, 3.0) = 5)
WHERE explain LIKE '%Granules: %/%' LIMIT 1;
SELECT 'equals', extract(explain, '(Name: idx|Granules: \d+/\d+)') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_price WHERE price = 1.1)
WHERE explain LIKE '%Granules: %/%' LIMIT 1;
SELECT 'exact comparison', toDecimal128('0.1000000000000000000001', 22) > 0.1;

DROP TABLE t_price;
DROP TABLE t_projection;
DROP TABLE t_key;
