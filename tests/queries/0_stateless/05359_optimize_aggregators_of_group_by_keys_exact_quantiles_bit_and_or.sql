-- Exact non-interpolating quantiles and idempotent groupBitAnd / groupBitOr of a GROUP BY key
-- equal the key, so `optimize_aggregators_of_group_by_keys` eliminates them like min/max/any.

SET enable_analyzer = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_agg_keys;
CREATE TABLE t_agg_keys (k UInt8, lc LowCardinality(UInt8), n Nullable(UInt8)) ENGINE = Memory;
INSERT INTO t_agg_keys SELECT number % 7, number % 5, if(number % 4 = 0, NULL, number % 3) FROM numbers(100);

SELECT '-- results with the optimization on and off';
SELECT k, quantileExact(k), medianExact(k), quantileExact(0.9)(k), quantileExactLow(k), quantileExactHigh(k), groupBitAnd(k), groupBitOr(k)
FROM t_agg_keys GROUP BY k ORDER BY k SETTINGS optimize_aggregators_of_group_by_keys = 1;
SELECT k, quantileExact(k), medianExact(k), quantileExact(0.9)(k), quantileExactLow(k), quantileExactHigh(k), groupBitAnd(k), groupBitOr(k)
FROM t_agg_keys GROUP BY k ORDER BY k SETTINGS optimize_aggregators_of_group_by_keys = 0;
SELECT lc, groupBitAnd(lc), groupBitOr(lc), toTypeName(groupBitOr(lc)) FROM t_agg_keys GROUP BY lc ORDER BY lc SETTINGS optimize_aggregators_of_group_by_keys = 1;
SELECT lc, groupBitAnd(lc), groupBitOr(lc), toTypeName(groupBitOr(lc)) FROM t_agg_keys GROUP BY lc ORDER BY lc SETTINGS optimize_aggregators_of_group_by_keys = 0;
SELECT n, quantileExact(n), groupBitAnd(n), groupBitOr(n) FROM t_agg_keys GROUP BY n ORDER BY n SETTINGS optimize_aggregators_of_group_by_keys = 1;
SELECT n, quantileExact(n), groupBitAnd(n), groupBitOr(n) FROM t_agg_keys GROUP BY n ORDER BY n SETTINGS optimize_aggregators_of_group_by_keys = 0;

SELECT '-- the aggregates are gone from the query tree';
SELECT countIf(explain LIKE '%function_type: aggregate%') FROM (
    EXPLAIN QUERY TREE
    SELECT quantileExact(k), quantileExactLow(k), quantileExactHigh(k), groupBitAnd(k), groupBitOr(k)
    FROM t_agg_keys GROUP BY k SETTINGS optimize_aggregators_of_group_by_keys = 1);

SELECT '-- groupBitXor and interpolating quantiles are not eliminated';
SELECT countIf(explain LIKE '%function_type: aggregate%') FROM (
    EXPLAIN QUERY TREE
    SELECT groupBitXor(k), quantileExactInclusive(k), quantile(k)
    FROM t_agg_keys GROUP BY k SETTINGS optimize_aggregators_of_group_by_keys = 1);

DROP TABLE t_agg_keys;
