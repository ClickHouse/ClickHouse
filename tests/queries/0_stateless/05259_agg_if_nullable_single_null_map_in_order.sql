-- `-If` aggregate functions with one Nullable argument, with aggregation in order, without a key and in window frames.

DROP TABLE IF EXISTS t_agg_if_one_null_map;

CREATE TABLE t_agg_if_one_null_map (id UInt32, k UInt32, x Nullable(Float64), y Float64, cond UInt8, cn Nullable(UInt8), wi Nullable(UInt64))
ENGINE = MergeTree ORDER BY (k, id);

-- Values are small integers, so the results do not depend on the order in which rows are added.
INSERT INTO t_agg_if_one_null_map SELECT
    number,
    intDiv(number, 8),
    if(number % 17 = 0, NULL, toFloat64(number % 1000)),
    toFloat64(number % 101),
    number % 3 != 0,
    if(number % 23 = 0, NULL, toUInt8(number % 3 != 0)),
    if(number % 11 = 0, NULL, toUInt64(number % 97) + 1)
FROM numbers(10000);

SELECT k, argMaxIf(y, x, cond), groupBitOrIf(wi, cond), varSampIf(x, cond), covarSampIf(x, y, cond), regr_slopeIf(y, x, cond),
    sumIf(x, cond), countIf(x, cond), minIf(x, cn), sumIfOrNullIf(x, cond, y > 50), countIfOrDefaultIf(cn, y > 50)
FROM t_agg_if_one_null_map GROUP BY k ORDER BY k LIMIT 10
SETTINGS optimize_aggregation_in_order = 1;

SELECT sumIfOrNullIf(x, cond, y > 50), countIfOrDefaultIf(cn, y > 50) FROM t_agg_if_one_null_map;

SELECT id, argMaxIf(y, x, cond) OVER w, groupBitOrIf(wi, cond) OVER w, varSampIf(x, cond) OVER w, covarSampIf(x, y, cond) OVER w,
    regr_slopeIf(y, x, cond) OVER w, sumIf(x, cond) OVER w, countIf(x, cond) OVER w, minIf(x, cn) OVER w
FROM t_agg_if_one_null_map WINDOW w AS (ORDER BY k, id ROWS BETWEEN 100 PRECEDING AND CURRENT ROW)
ORDER BY id DESC LIMIT 10;

DROP TABLE t_agg_if_one_null_map;
