DROP TABLE IF EXISTS t_agg_if_span;

CREATE TABLE t_agg_if_span (id UInt32, k UInt32, x Nullable(Float64), y Float64, cond UInt8, cn Nullable(UInt8))
ENGINE = MergeTree ORDER BY (k, id);

-- Values are integer-valued Float64 and groups are small, so every sum compared below is exact
-- and independent of accumulation order.
INSERT INTO t_agg_if_span SELECT
    number,
    intDiv(number, 8),
    if(number % 17 = 0, NULL, toFloat64(number % 1000)),
    toFloat64(number % 1000),
    number % 3 != 0,
    if(number % 23 = 0, NULL, toUInt8(number % 3 != 0))
FROM numbers(10000);

SET optimize_aggregation_in_order = 1;
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT k, sumIfOrDefault(x, cond) AS a, avgIfOrDefault(x, cond) AS b, sumIf(y, cn) AS c FROM t_agg_if_span GROUP BY k) WHERE explain ILIKE '%AggregatingInOrderTransform%';
CREATE TEMPORARY TABLE in_order AS SELECT k, sumIfOrDefault(x, cond) AS a, avgIfOrDefault(x, cond) AS b, sumIf(y, cn) AS c FROM t_agg_if_span GROUP BY k;
SET optimize_aggregation_in_order = 0;
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT k, sumIfOrDefault(x, cond) AS a, avgIfOrDefault(x, cond) AS b, sumIf(y, cn) AS c FROM t_agg_if_span GROUP BY k) WHERE explain ILIKE '%AggregatingInOrderTransform%';
CREATE TEMPORARY TABLE by_hash AS SELECT k, sumIfOrDefault(x, cond) AS a, avgIfOrDefault(x, cond) AS b, sumIf(y, cn) AS c FROM t_agg_if_span GROUP BY k;
SELECT count() FROM (SELECT * FROM in_order EXCEPT SELECT * FROM by_hash);

CREATE TEMPORARY TABLE frame_default AS SELECT id, sumIfOrDefault(x, cond) OVER w AS a, sumIf(y, cn) OVER w AS c FROM t_agg_if_span WINDOW w AS (ORDER BY k, id ROWS BETWEEN 100 PRECEDING AND CURRENT ROW);
CREATE TEMPORARY TABLE frame_small_blocks AS SELECT id, sumIfOrDefault(x, cond) OVER w AS a, sumIf(y, cn) OVER w AS c FROM t_agg_if_span WINDOW w AS (ORDER BY k, id ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) SETTINGS max_block_size = 997;
SELECT count() FROM (SELECT * FROM frame_default EXCEPT SELECT * FROM frame_small_blocks);

DROP TABLE t_agg_if_span;
