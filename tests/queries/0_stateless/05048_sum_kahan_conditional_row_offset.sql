SELECT n, sumIf(x, cond) OVER w AS s, sumKahanIf(x, cond) OVER w AS sk
FROM (SELECT number AS n, toFloat64(1) AS x, toUInt8(number < 3) AS cond FROM numbers(6))
WINDOW w AS (ORDER BY n ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
ORDER BY n;

SELECT n, sum(x) OVER w AS s, sumKahan(x) OVER w AS sk
FROM (SELECT number AS n, if(number < 3, toFloat64(1), NULL) AS x FROM numbers(6))
WINDOW w AS (ORDER BY n ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
ORDER BY n;

SELECT n, sumIf(x, cond) OVER w AS s, sumKahanIf(x, cond) OVER w AS sk
FROM (SELECT number AS n, if(number % 2 = 0, toFloat64(1), NULL) AS x, toUInt8(number < 4) AS cond FROM numbers(6))
WINDOW w AS (ORDER BY n ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
ORDER BY n;

SELECT n, sumIf(x, cond) OVER w AS s, sumKahanIf(x, cond) OVER w AS sk
FROM (SELECT number AS n, toFloat64(1) AS x, if(number % 3 = 0, NULL, toUInt8(number < 4)) AS cond FROM numbers(6))
WINDOW w AS (ORDER BY n ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
ORDER BY n;

DROP TABLE IF EXISTS t_sum_kahan_offset;

CREATE TABLE t_sum_kahan_offset (k UInt32, x Float64, cond UInt8) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_sum_kahan_offset SELECT intDiv(number, 32), toFloat64(1), toUInt8(number < 32) FROM numbers(128);

SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT k, sumKahanIf(x, cond) FROM t_sum_kahan_offset GROUP BY k
    SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_max_block_bytes = 50000000
) WHERE explain ILIKE '%AggregatingInOrder%';

SELECT k, sumIf(x, cond) AS s, sumKahanIf(x, cond) AS sk
FROM t_sum_kahan_offset
GROUP BY k
ORDER BY k
SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_max_block_bytes = 50000000;

DROP TABLE t_sum_kahan_offset;
