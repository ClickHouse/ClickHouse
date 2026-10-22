-- Aggregate functions that don't support Variant are applied to Nullable of the least common supertype of the alternatives.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_variant_agg;
CREATE TABLE t_variant_agg (k UInt8, v Variant(UInt8, Int32, Float64)) ENGINE = Memory;
INSERT INTO t_variant_agg VALUES (0, 1::UInt8), (0, -2::Int32), (0, NULL), (1, 3.5::Float64), (1, NULL), (1, 10::UInt8);

SELECT sum(v), avg(v), min(v), max(v), toTypeName(sum(v)), toTypeName(min(v)) FROM t_variant_agg;
SELECT k, sum(v), min(v), max(v), quantileExact(v) FROM t_variant_agg GROUP BY k ORDER BY k;
SELECT sumIf(v, k = 1), argMax(k, v), argMin(k, v), maxArgMin(v, v) FROM t_variant_agg;
SELECT sumMerge(s) FROM (SELECT sumState(v) AS s FROM t_variant_agg GROUP BY k);
SELECT sum(v) FROM t_variant_agg WHERE v IS NULL;

-- Functions that support Variant natively are not affected.
SELECT count(v), groupArray(v), toTypeName(any(v)) FROM t_variant_agg;
-- Only the arguments that are not supported are converted: the first argument of argMax and topKWeighted is processed natively.
SELECT toTypeName(argMax(v, k)), toTypeName(topKWeighted(v, k)), toTypeName(argMax(k, v)), toTypeName(argMax(v, v)) FROM t_variant_agg;

-- Window functions.
SELECT DISTINCT k, sum(v) OVER (PARTITION BY k) FROM t_variant_agg ORDER BY k;

-- Ordered windows add rows incrementally, the rows of the frame are converted on every step.
SELECT k, n, sum(v) OVER (PARTITION BY k ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), max(v) OVER (PARTITION BY k ORDER BY n ROWS UNBOUNDED PRECEDING)
FROM (SELECT number % 2 AS k, number AS n, if(number % 3 = 0, NULL, number::Variant(UInt64)) AS v FROM numbers(8))
ORDER BY k, n;
SELECT sum(s) FROM (SELECT sum(v) OVER (ORDER BY number ROWS UNBOUNDED PRECEDING) AS s FROM (SELECT number, number::Variant(UInt64) AS v FROM numbers(20000)))
SETTINGS max_block_size = 20000, max_threads = 1;

-- Explicitly declared state of a function with a Variant argument.
DROP TABLE IF EXISTS t_variant_agg_state;
CREATE TABLE t_variant_agg_state (s AggregateFunction(sum, Variant(UInt64))) ENGINE = Memory;
INSERT INTO t_variant_agg_state SELECT sumState(number::Variant(UInt64)) FROM numbers(3);
INSERT INTO t_variant_agg_state SELECT sumState(number::Variant(UInt64)) FROM numbers(5);
SELECT finalizeAggregation(s) FROM t_variant_agg_state ORDER BY 1;
SELECT sumMerge(s), finalizeAggregation(sumMergeState(s)), toTypeName(sumMergeState(s)) FROM t_variant_agg_state;
DROP TABLE t_variant_agg_state;

-- Combinators that expose the Variant only to the nested function.
SELECT sumArray([1::Variant(UInt64), NULL, 2::Variant(UInt64)]), maxForEach([number::Variant(UInt64), NULL]) FROM numbers(3);

SELECT max(number::Variant(UInt64)), min(number::Variant(UInt64)) FROM numbers(10);
SELECT argMax(number, number::Variant(UInt64)), argMin(number, number::Variant(UInt64)) FROM numbers(10);
SELECT anyArgMax(number, number::Variant(UInt64)), anyArgMin(number, number::Variant(UInt64)) FROM numbers(10);
SELECT min(v), max(v) FROM (SELECT '2020-01-01'::Date::Variant(Date, DateTime) AS v UNION ALL SELECT '2021-01-01 00:00:00'::DateTime::Variant(Date, DateTime));

-- Nullable condition of -If with native and converted aggregate functions.
SELECT uniqIf(number::Variant(UInt64), toNullable(number % 2 = 0)), anyIf(number::Variant(UInt64), toNullable(number % 2 = 1)), sumIf(number::Variant(UInt64), toNullable(number % 2 = 0)) FROM numbers(4);
SELECT sumIf(number::Variant(UInt64), if(number = 2, NULL, toNullable(number % 2 = 0))) FROM numbers(5);
SELECT number % 3 AS k, sumIf(number::Variant(UInt64), number % 2 = 0), maxIf(number::Variant(UInt64), toNullable(number % 2 = 1)) FROM numbers(10) GROUP BY k ORDER BY k;

-- Only the key of -ArgMax, argMax and the weight of topKWeighted are converted, the payload keeps its type and NULL values.
SELECT groupArrayArgMax(v, k), groupArrayArgMax(v, CAST(k AS Nullable(UInt8))), argMax(v, k), toTypeName(argMax(v, k))
FROM values('v Variant(UInt64), k Variant(UInt8)', (NULL, 2), (1, 1));
SELECT argMax(v, k), argMax(v, CAST(k AS Nullable(UInt8))), topKWeighted(2)(v, k), groupArrayResample(0, 2, 1)(v, k)
FROM values('v Variant(String, UInt64), k Variant(UInt8)', ('a', 1), (2, 0), ('a', 3));

-- The NULL values of the Variant are skipped before -ArgMax, -OrDefault and -Resample, as for the explicit cast to Nullable.
SELECT
    sumArgMax(v, k), sumArgMax(CAST(v AS Nullable(UInt64)), k),
    sumIfArgMin(v, 1, k), sumIfArgMin(CAST(v AS Nullable(UInt64)), 1, k),
    sumIfOrDefault(v, 0), sumIfOrDefault(CAST(v AS Nullable(UInt64)), 0),
    sumOrDefault(v), sumOrDefault(CAST(v AS Nullable(UInt64))),
    sumResample(0, 3, 1)(v, k), sumResample(0, 3, 1)(CAST(v AS Nullable(UInt64)), k)
FROM values('v Variant(UInt64), k UInt8', (NULL, 2), (3, 1), (NULL, 0));
SELECT sumIfOrDefault(v, toNullable(1)), sumOrNull(v), groupArrayArgMax(v, k), toTypeName(sumArgMax(v, k))
FROM values('v Variant(UInt64), k UInt8', (NULL, 2), (3, 1));

-- DISTINCT distinguishes equal values of different alternatives, as a distinct subquery does.
SELECT sum(DISTINCT v), sumDistinct(v), sumDistinctIf(v, 1), uniqExact(v), (SELECT sum(v) FROM (SELECT DISTINCT v FROM (SELECT arrayJoin([1::UInt8::Variant(UInt8, UInt16), 1::UInt16::Variant(UInt8, UInt16)]) AS v)))
FROM (SELECT arrayJoin([1::UInt8::Variant(UInt8, UInt16), 1::UInt16::Variant(UInt8, UInt16)]) AS v);

-- No common supertype.
SELECT sum(v) FROM values('v Variant(String, UInt64)', 'a', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT min([1]::Array(UInt8)::Variant(Array(UInt8), Array(UInt16))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_variant_agg;
