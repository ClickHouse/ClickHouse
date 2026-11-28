-- Index analysis evaluates a key expression at the endpoints of the key ranges. Mixed `Decimal`/integer
-- arithmetic raises `DECIMAL_OVERFLOW` for an endpoint that does not fit into the native width of the
-- `Decimal` result, which must only make the condition unusable for the index, not fail the query: another
-- condition restricts the rows read to a safe slice of the key.

DROP TABLE IF EXISTS t_decimal_overflow_key;
CREATE TABLE t_decimal_overflow_key (a Int64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_decimal_overflow_key VALUES (0), (1), (2), (3000000000);

SELECT a FROM t_decimal_overflow_key WHERE a < 2 AND a + toDecimal32(1, 0) BETWEEN 1 AND 3 ORDER BY a;
SELECT a FROM t_decimal_overflow_key WHERE a < 2 AND toDecimal32(1, 0) + a BETWEEN 1 AND 3 ORDER BY a;
SELECT a FROM t_decimal_overflow_key WHERE a < 2 AND a - toDecimal32(1, 0) BETWEEN -1 AND 0 ORDER BY a;
SELECT a FROM t_decimal_overflow_key WHERE a < 2 AND toDecimal32(5, 0) - a BETWEEN 4 AND 5 ORDER BY a;
SELECT a FROM t_decimal_overflow_key WHERE a < 2 AND a + toDecimal32(1, 4) IN (1, 2) ORDER BY a;

SELECT trimLeft(explain) AS s FROM (EXPLAIN indexes = 1 SELECT a FROM t_decimal_overflow_key WHERE a < 2 AND a + toDecimal32(1, 0) BETWEEN 1 AND 3 SETTINGS enable_parallel_replicas = 0)
WHERE s LIKE 'Granules: %/%';

-- The row that does not fit is still reported when it is read.
SELECT a FROM t_decimal_overflow_key WHERE a + toDecimal32(1, 0) = 2; -- { serverError DECIMAL_OVERFLOW }

DROP TABLE t_decimal_overflow_key;
