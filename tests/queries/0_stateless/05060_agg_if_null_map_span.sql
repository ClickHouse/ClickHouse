DROP TABLE IF EXISTS t_agg_if_span;

CREATE TABLE t_agg_if_span (id UInt32, k UInt32, x Nullable(Float64), y Float64, cond UInt8, cn Nullable(UInt8), w Nullable(Float64), wi Nullable(UInt64))
ENGINE = MergeTree ORDER BY (k, id);

-- Values are integer-valued Float64 and groups are small, so every sum compared below is exact
-- and independent of accumulation order.
INSERT INTO t_agg_if_span SELECT
    number,
    intDiv(number, 8),
    if(number % 17 = 0, NULL, toFloat64(number % 1000)),
    toFloat64(number % 1000),
    number % 3 != 0,
    if(number % 23 = 0, NULL, toUInt8(number % 3 != 0)),
    if(number % 13 = 0, NULL, toFloat64(number % 97)),
    if(number % 11 = 0, NULL, toUInt64(number % 97) + 1)
FROM numbers(10000);

SET optimize_aggregation_in_order = 1;
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT k, sumIfOrDefault(x, cond) AS a, avgIfOrDefault(x, cond) AS b, sumIf(y, cn) AS c FROM t_agg_if_span GROUP BY k) WHERE explain ILIKE '%AggregatingInOrderTransform%';
CREATE TEMPORARY TABLE in_order AS SELECT k, sumIfOrDefault(x, cond) AS a, avgIfOrDefault(x, cond) AS b, sumIf(y, cn) AS c FROM t_agg_if_span GROUP BY k;
SET optimize_aggregation_in_order = 0;
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT k, sumIfOrDefault(x, cond) AS a, avgIfOrDefault(x, cond) AS b, sumIf(y, cn) AS c FROM t_agg_if_span GROUP BY k) WHERE explain ILIKE '%AggregatingInOrderTransform%';
CREATE TEMPORARY TABLE by_hash AS SELECT k, sumIfOrDefault(x, cond) AS a, avgIfOrDefault(x, cond) AS b, sumIf(y, cn) AS c FROM t_agg_if_span GROUP BY k;
SELECT count() FROM (SELECT * FROM in_order EXCEPT SELECT * FROM by_hash);

-- The other flag buffers merged once per sub-range: the -If null-map adapters, `SingleValueData`,
-- the two-Nullable-argument merge, and the TimeSeries -If kernels.
SET allow_experimental_time_series_aggregate_functions = 1;
SET optimize_aggregation_in_order = 1;
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT k, minIf(x, cond) AS a, argMinIf(y, x, cond) AS c, quantileExactWeighted(x, wi) AS d FROM t_agg_if_span GROUP BY k) WHERE explain ILIKE '%AggregatingInOrderTransform%';
CREATE TEMPORARY TABLE in_order2 AS SELECT k, minIf(x, cond) AS a, maxIf(x, cond) AS b, argMinIf(y, x, cond) AS c, quantileExactWeighted(x, wi) AS d,
    toString(timeSeriesLastTwoSamplesIf(toDateTime64(id, 3), x, cond)) AS e,
    toString(timeSeriesGroupArrayIf(toDateTime64(id, 3), x, cond)) AS f,
    toString(timeSeriesRateToGridIf(toDateTime64(1, 3), toDateTime64(10000, 3), toIntervalSecond(400), toIntervalSecond(2000))(toDateTime64(id, 3), x, cond)) AS g
FROM t_agg_if_span GROUP BY k;
SET optimize_aggregation_in_order = 0;
CREATE TEMPORARY TABLE by_hash2 AS SELECT k, minIf(x, cond) AS a, maxIf(x, cond) AS b, argMinIf(y, x, cond) AS c, quantileExactWeighted(x, wi) AS d,
    toString(timeSeriesLastTwoSamplesIf(toDateTime64(id, 3), x, cond)) AS e,
    toString(timeSeriesGroupArrayIf(toDateTime64(id, 3), x, cond)) AS f,
    toString(timeSeriesRateToGridIf(toDateTime64(1, 3), toDateTime64(10000, 3), toIntervalSecond(400), toIntervalSecond(2000))(toDateTime64(id, 3), x, cond)) AS g
FROM t_agg_if_span GROUP BY k;
SELECT count() FROM (SELECT * FROM in_order2 EXCEPT SELECT * FROM by_hash2);

-- Both arms above build the same merged buffer, so neither can see a buffer read before it is
-- written. `quantileExactWeighted` over two Nullable arguments is exact, so assert its value.
SET optimize_aggregation_in_order = 1;
SELECT toInt64(sum(ifNull(s, -1))) FROM (SELECT k, quantileExactWeighted(x, wi) AS s FROM t_agg_if_span GROUP BY k);
SELECT toInt64(sum(ifNull(s, -1))) FROM (SELECT k, quantileExactWeighted(x, wi) AS s FROM t_agg_if_span GROUP BY k SETTINGS optimize_aggregation_in_order = 0);

CREATE TEMPORARY TABLE frame_default AS SELECT id, sumIfOrDefault(x, cond) OVER w AS a, sumIf(y, cn) OVER w AS c, minIf(x, cond) OVER w AS d, quantileExactWeighted(x, wi) OVER w AS e FROM t_agg_if_span WINDOW w AS (ORDER BY k, id ROWS BETWEEN 100 PRECEDING AND CURRENT ROW);
CREATE TEMPORARY TABLE frame_small_blocks AS SELECT id, sumIfOrDefault(x, cond) OVER w AS a, sumIf(y, cn) OVER w AS c, minIf(x, cond) OVER w AS d, quantileExactWeighted(x, wi) OVER w AS e FROM t_agg_if_span WINDOW w AS (ORDER BY k, id ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) SETTINGS max_block_size = 997;
SELECT count() FROM (SELECT * FROM frame_default EXCEPT SELECT * FROM frame_small_blocks);
SELECT toInt64(sum(ifNull(e, -1))) FROM frame_default;

DROP TABLE t_agg_if_span;
