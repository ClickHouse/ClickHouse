-- `-If` aggregate functions with one Nullable argument give the same result with aggregation in order, where a group
-- is added as a range of rows of a block, as with hash aggregation, and the same result in window frames for any block size.

DROP TABLE IF EXISTS t_agg_if_one_null_map;

CREATE TABLE t_agg_if_one_null_map (id UInt32, k UInt32, x Nullable(Float64), y Float64, cond UInt8, cn Nullable(UInt8), wi Nullable(UInt64))
ENGINE = MergeTree ORDER BY (k, id);

-- Values are small integers, so the sums behind every result compared below are exact and independent of the order
-- in which rows are added.
INSERT INTO t_agg_if_one_null_map SELECT
    number,
    intDiv(number, 8),
    if(number % 17 = 0, NULL, toFloat64(number % 1000)),
    toFloat64(number % 101),
    number % 3 != 0,
    if(number % 23 = 0, NULL, toUInt8(number % 3 != 0)),
    if(number % 11 = 0, NULL, toUInt64(number % 97) + 1)
FROM numbers(10000);

SET optimize_aggregation_in_order = 1;
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT k, argMaxIf(y, x, cond) FROM t_agg_if_one_null_map GROUP BY k) WHERE explain ILIKE '%AggregatingInOrderTransform%';
CREATE TEMPORARY TABLE in_order AS SELECT k,
    argMaxIf(y, x, cond) AS a, groupBitOrIf(wi, cond) AS b, varSampIf(x, cond) AS c, covarSampIf(x, y, cond) AS d,
    regr_slopeIf(y, x, cond) AS e, sumIf(x, cond) AS f, countIf(x, cond) AS g, minIf(x, cn) AS h, sumIfOrNullIf(x, cond, y > 50) AS i, countIfOrDefaultIf(cn, y > 50) AS j
FROM t_agg_if_one_null_map GROUP BY k;
SET optimize_aggregation_in_order = 0;
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT k, argMaxIf(y, x, cond) FROM t_agg_if_one_null_map GROUP BY k) WHERE explain ILIKE '%AggregatingInOrderTransform%';
CREATE TEMPORARY TABLE by_hash AS SELECT k,
    argMaxIf(y, x, cond) AS a, groupBitOrIf(wi, cond) AS b, varSampIf(x, cond) AS c, covarSampIf(x, y, cond) AS d,
    regr_slopeIf(y, x, cond) AS e, sumIf(x, cond) AS f, countIf(x, cond) AS g, minIf(x, cn) AS h, sumIfOrNullIf(x, cond, y > 50) AS i, countIfOrDefaultIf(cn, y > 50) AS j
FROM t_agg_if_one_null_map GROUP BY k;
SELECT count() FROM (SELECT * FROM in_order EXCEPT SELECT * FROM by_hash);

-- Both arms of every comparison can share a defect, so check two totals against a plain filter.
SELECT sum(f), sum(g) FROM in_order;
SELECT sum(x), count(x) FROM t_agg_if_one_null_map WHERE cond;

-- `-If` under `-OrNull` or `-OrDefault` under `-If` applies both conditions.
SELECT sumIfOrNullIf(x, cond, y > 50), countIfOrDefaultIf(cn, y > 50) FROM t_agg_if_one_null_map;
SELECT (SELECT sum(x) FROM t_agg_if_one_null_map WHERE cond AND y > 50), (SELECT count() FROM t_agg_if_one_null_map WHERE cn = 1 AND y > 50);

CREATE TEMPORARY TABLE frame_default AS SELECT id,
    argMaxIf(y, x, cond) OVER w AS a, groupBitOrIf(wi, cond) OVER w AS b, varSampIf(x, cond) OVER w AS c, covarSampIf(x, y, cond) OVER w AS d,
    regr_slopeIf(y, x, cond) OVER w AS e, sumIf(x, cond) OVER w AS f, countIf(x, cond) OVER w AS g, minIf(x, cn) OVER w AS h
FROM t_agg_if_one_null_map WINDOW w AS (ORDER BY k, id ROWS BETWEEN 100 PRECEDING AND CURRENT ROW);
CREATE TEMPORARY TABLE frame_small_blocks AS SELECT id,
    argMaxIf(y, x, cond) OVER w AS a, groupBitOrIf(wi, cond) OVER w AS b, varSampIf(x, cond) OVER w AS c, covarSampIf(x, y, cond) OVER w AS d,
    regr_slopeIf(y, x, cond) OVER w AS e, sumIf(x, cond) OVER w AS f, countIf(x, cond) OVER w AS g, minIf(x, cn) OVER w AS h
FROM t_agg_if_one_null_map WINDOW w AS (ORDER BY k, id ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) SETTINGS max_block_size = 997;
SELECT count() FROM (SELECT * FROM frame_default EXCEPT SELECT * FROM frame_small_blocks);

DROP TABLE t_agg_if_one_null_map;
