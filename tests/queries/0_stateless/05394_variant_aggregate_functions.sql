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

-- No common supertype.
SELECT sum(v) FROM values('v Variant(String, UInt64)', 'a', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT min([1]::Array(UInt8)::Variant(Array(UInt8), Array(UInt16))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_variant_agg;
