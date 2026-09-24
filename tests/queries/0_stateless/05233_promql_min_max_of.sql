-- Tags: no-fasttest
-- PromQL needs ANTLR4, which is disabled in the fast-test build.

SET enable_time_series_table = 1;
SET enable_time_series_aggregate_functions = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS promql_min_max;
CREATE TABLE promql_min_max ENGINE = TimeSeries;

SELECT '-- constants, arithmetic and nested calls';
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(3, 5)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(5, 3)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(4, 4)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(-2, -5)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(1.25, -2.5)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(0, 1)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(3, 5)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(5, 3)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(4, 4)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(-2, -5)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(1.25, -2.5)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(0, 1)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(max_of(-2, 7), 5)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(min_of(7, 3), 5)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(2 + 3, 2 * 4)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(2 + 3, 2 * 4)', 100);
SELECT tags, value FROM prometheusQuery(promql_min_max, 'vector(min_of(3, 5))', 100);
SELECT tags, value FROM prometheusQuery(promql_min_max, 'clamp_min(vector(1), max_of(2, 3))', 100);

SELECT '-- NaN and infinities in constants';
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(NaN, 3)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(3, NaN)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(NaN, NaN)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(-Inf, Inf)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(NaN, -Inf)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(-Inf, NaN)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(NaN, Inf)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(Inf, NaN)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(NaN, 3)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(3, NaN)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(NaN, NaN)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(-Inf, Inf)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(NaN, -Inf)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(-Inf, NaN)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(NaN, Inf)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(Inf, NaN)', 100);

SELECT '-- signed zero in either argument order';
SELECT value FROM prometheusQuery(promql_min_max, '1 / min_of(0, 0)', 100);
SELECT value FROM prometheusQuery(promql_min_max, '1 / min_of(0, -0)', 100);
SELECT value FROM prometheusQuery(promql_min_max, '1 / min_of(-0, 0)', 100);
SELECT value FROM prometheusQuery(promql_min_max, '1 / min_of(-0, -0)', 100);
SELECT value FROM prometheusQuery(promql_min_max, '1 / max_of(0, 0)', 100);
SELECT value FROM prometheusQuery(promql_min_max, '1 / max_of(0, -0)', 100);
SELECT value FROM prometheusQuery(promql_min_max, '1 / max_of(-0, 0)', 100);
SELECT value FROM prometheusQuery(promql_min_max, '1 / max_of(-0, -0)', 100);

SELECT '-- constant and changing scalar grids';
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(promql_min_max, 'min_of(time() - 100, 2)', 100, 104, 1);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(promql_min_max, 'min_of(2, time() - 100)', 100, 104, 1);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(promql_min_max, 'max_of(time() - 100, 2)', 100, 104, 1);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(promql_min_max, 'max_of(2, time() - 100)', 100, 104, 1);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(promql_min_max, 'min_of(time() - 100, 104 - time())', 100, 104, 1);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(promql_min_max, 'max_of(time() - 100, 104 - time())', 100, 104, 1);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(promql_min_max, 'min_of(max_of(time() - 101, 0), 2)', 100, 104, 1);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(promql_min_max, 'max_of(1 + 1, time() - 100)', 100, 104, 1);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(promql_min_max, 'min_of(3, 5)', 100, 104, 1);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(promql_min_max, 'max_of(3, 5)', 100, 104, 1);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(promql_min_max, 'min_of(time() - 100, 2)', 104, 104, 1);

SELECT '-- runtime scalar inputs and all pairs of special values';
-- Every ordered pair from [NaN, -Inf, negative, -0, +0, positive, +Inf].
INSERT INTO promql_min_max (metric_name, samples)
WITH [nan, -inf, -3.5, toFloat64('-0'), 0., 3.5, inf] AS vals
SELECT 'lhs', arrayMap(i -> (toDateTime64(100 + i, 3), vals[intDiv(i, 7) + 1]), range(49))
UNION ALL
SELECT 'rhs', arrayMap(i -> (toDateTime64(100 + i, 3), vals[i % 7 + 1]), range(49));
SELECT arrayMap(x -> toString(x.2), samples)
FROM prometheusQueryRange(promql_min_max, 'min_of(scalar(lhs), scalar(rhs))', 100, 148, 1);
SELECT arrayMap(x -> toString(x.2), samples)
FROM prometheusQueryRange(promql_min_max, 'max_of(scalar(lhs), scalar(rhs))', 100, 148, 1);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(scalar(lhs), scalar(rhs))', 116);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(scalar(lhs), scalar(rhs))', 116);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(scalar(lhs), 2)', 116);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(2, scalar(lhs))', 116);

SELECT '-- empty and multiple-series scalar inputs';
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(scalar(missing), 1)', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(1, scalar(missing))', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of(scalar({__name__=~"lhs|rhs"}), 1)', 116);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of(1, scalar({__name__=~"lhs|rhs"}))', 116);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(promql_min_max, 'min_of(scalar(missing), time())', 100, 104, 1);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(promql_min_max, 'max_of(time(), scalar(missing))', 100, 104, 1);

SELECT '-- function names remain valid metric and label names';
INSERT INTO promql_min_max (metric_name, tags, samples) VALUES
    ('min_of', map('max_of', 'yes'), [(toDateTime64(100, 3), 7)]),
    ('max_of', map('min_of', 'yes'), [(toDateTime64(100, 3), 8)]);
SELECT value FROM prometheusQuery(promql_min_max, 'min_of{max_of="yes"}', 100);
SELECT value FROM prometheusQuery(promql_min_max, 'max_of{min_of="yes"}', 100);

SELECT '-- argument count and type errors';
SELECT * FROM prometheusQuery(promql_min_max, 'min_of()', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'min_of(1)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'min_of(1, 2, 3)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'min_of(vector(1), 2)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'min_of(1, vector(2))', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'min_of(lhs[1m], 2)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'min_of(1, rhs[1m])', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'min_of("a", 2)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'min_of(1, "b")', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'max_of()', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'max_of(1)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'max_of(1, 2, 3)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'max_of(vector(1), 2)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'max_of(1, vector(2))', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'max_of(lhs[1m], 2)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'max_of(1, rhs[1m])', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'max_of("a", 2)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(promql_min_max, 'max_of(1, "b")', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- Float32 samples';
DROP TABLE IF EXISTS promql_min_max_float32;
CREATE TABLE promql_min_max_float32 (samples Array(Tuple(DateTime64(3, 'UTC'), Float32))) ENGINE = TimeSeries;
INSERT INTO promql_min_max_float32 (metric_name, samples)
SELECT metric_name, samples FROM promql_min_max WHERE metric_name IN ('lhs', 'rhs');
SELECT toTypeName(value), value FROM prometheusQuery(promql_min_max_float32, 'min_of(scalar(lhs), 2)', 116);
SELECT toTypeName(value), value FROM prometheusQuery(promql_min_max_float32, 'max_of(2, scalar(lhs))', 116);
SELECT arrayMap(x -> toString(x.2), samples)
FROM prometheusQueryRange(promql_min_max_float32, 'min_of(scalar(lhs), scalar(rhs))', 100, 148, 1);
SELECT arrayMap(x -> toString(x.2), samples)
FROM prometheusQueryRange(promql_min_max_float32, 'max_of(scalar(lhs), scalar(rhs))', 100, 148, 1);

DROP TABLE promql_min_max_float32;
DROP TABLE promql_min_max;
