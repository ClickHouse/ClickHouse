-- Aggregate functions that do not support the Variant type natively (sum, avg, min, max, ...) can now be applied
-- to Variant arguments: they aggregate over the least common supertype of the variants, wrapped in Nullable.
-- A mix of numeric types (e.g. Decimal + Float64) is aggregated in Float64, exactly as arithmetic does.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;
SET allow_suspicious_types_in_group_by = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS t_variant_agg;
CREATE TABLE t_variant_agg (id UInt64, v Variant(Decimal(7, 2), Float64)) ENGINE = Memory;
INSERT INTO t_variant_agg SELECT number, number::Decimal(7, 2) FROM numbers(3);        -- 0, 1, 2
INSERT INTO t_variant_agg SELECT number + 10, (number + 0.5)::Float64 FROM numbers(3);  -- 0.5, 1.5, 2.5
INSERT INTO t_variant_agg VALUES (100, NULL);

-- The motivating example: sum over Variant(Decimal(7, 2), Float64) is aggregated over the supertype Float64.
SELECT 'sum', sum(v), toTypeName(sum(v)) FROM t_variant_agg;
SELECT 'avg', avg(v), toTypeName(avg(v)) FROM t_variant_agg;
SELECT 'min/max', min(v), max(v) FROM t_variant_agg;
SELECT 'sumWithOverflow', sumWithOverflow(v) FROM t_variant_agg;
SELECT 'sumKahan', sumKahan(v) FROM t_variant_agg;

-- Combinators are applied around the adapter (the adapter is the outermost wrapper).
SELECT 'sumIf', sumIf(v, id < 10) FROM t_variant_agg;
SELECT 'sumState/Merge', sumMerge(s) FROM (SELECT sumState(v) AS s FROM t_variant_agg);

-- GROUP BY.
SELECT 'groupBy', id < 10 AS g, sum(v) FROM t_variant_agg GROUP BY g ORDER BY g;

DROP TABLE t_variant_agg;

-- A clean common supertype is preserved (no Float64 fallback): Variant(UInt8, UInt32) -> UInt32 -> sum -> UInt64.
DROP TABLE IF EXISTS t_variant_uint;
CREATE TABLE t_variant_uint (v Variant(UInt8, UInt32)) ENGINE = Memory;
INSERT INTO t_variant_uint SELECT number::UInt8 FROM numbers(3);
INSERT INTO t_variant_uint SELECT (number + 1000)::UInt32 FROM numbers(3);
SELECT 'uint', sum(v), toTypeName(sum(v)) FROM t_variant_uint;
DROP TABLE t_variant_uint;

-- All-NULL and empty aggregation.
DROP TABLE IF EXISTS t_variant_null;
CREATE TABLE t_variant_null (v Variant(Int64, Float64)) ENGINE = Memory;
INSERT INTO t_variant_null VALUES (NULL), (NULL);
SELECT 'all null', sum(v), min(v), avg(v) FROM t_variant_null;
SELECT 'empty', sum(v), min(v) FROM t_variant_null WHERE 0;
DROP TABLE t_variant_null;

-- Functions that already support Variant natively are unaffected (no adapter, result type is kept).
DROP TABLE IF EXISTS t_variant_native;
CREATE TABLE t_variant_native (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO t_variant_native VALUES (1), ('a'), (2), (NULL);
SELECT 'native', count(v), toTypeName(any(v)), uniqExact(v) FROM t_variant_native;

-- Variants without a common numeric supertype are still rejected with the original error.
SELECT sum(v) FROM t_variant_native; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT avg(v) FROM t_variant_native; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE t_variant_native;

-- The -Merge combinator over an AggregateFunction(f, Variant(...)) state type. Such a state type is constructible
-- (e.g. as the type of a non-final aggregation block, which keeps the original Variant argument list) and
-- reconstructible, so -Merge must resolve its nested function through the same Variant adapter, otherwise it would
-- throw ILLEGAL_TYPE_OF_ARGUMENT when reconstructing the state.
DROP TABLE IF EXISTS t_variant_state;
CREATE TABLE t_variant_state
(
    s AggregateFunction(sum, Variant(Int64, Float64)),
    mn AggregateFunction(min, Variant(Int64, Float64)),
    mx AggregateFunction(max, Variant(Int64, Float64)),
    a AggregateFunction(avg, Variant(Int64, Float64))
) ENGINE = Memory;
SELECT 'merge empty', sumMerge(s), minMerge(mn), maxMerge(mx), avgMerge(a) FROM t_variant_state;
SELECT 'merge types', toTypeName(sumMerge(s)), toTypeName(minMerge(mn)), toTypeName(avgMerge(a)) FROM t_variant_state;
DROP TABLE t_variant_state;

-- A clean common supertype is preserved for the state type too: Variant(UInt8, UInt32) -> UInt32 -> sum -> UInt64.
DROP TABLE IF EXISTS t_variant_state_uint;
CREATE TABLE t_variant_state_uint (s AggregateFunction(sum, Variant(UInt8, UInt32))) ENGINE = Memory;
SELECT 'merge uint', toTypeName(sumMerge(s)) FROM t_variant_state_uint;
DROP TABLE t_variant_state_uint;

-- A state type over a Variant without a common numeric supertype is still rejected with the original error.
CREATE TABLE t_variant_state_bad (s AggregateFunction(sum, Variant(Int64, String))) ENGINE = Memory; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The adapter is state-transparent: it shares the nested function's state bytes, so (like the -If and -Array
-- combinators) an AggregateFunction(f, Variant(...)) state normalizes to the same state as the byte-compatible
-- AggregateFunction(f, Nullable(supertype)) it delegates to. Such states can therefore be cast to one another and
-- unified -- the state-representation property tested for -If/-Array by 02366_normalize_aggregate_function_types_and_states.
DROP TABLE IF EXISTS t_variant_norm;
CREATE TABLE t_variant_norm (v Variant(Int64, Float64)) ENGINE = Memory;
INSERT INTO t_variant_norm VALUES (1), (2.5), (3), (NULL);
-- A Nullable(supertype)-form state casts to the Variant-form state type, and back.
SELECT 'cast to variant', sumMerge(CAST(s, 'AggregateFunction(sum, Variant(Int64, Float64))')) FROM (SELECT sumState(v) AS s FROM t_variant_norm);
SELECT 'cast from variant', sumMerge(CAST(s, 'AggregateFunction(sum, Nullable(Float64))')) FROM (SELECT CAST(sumState(v), 'AggregateFunction(sum, Variant(Int64, Float64))') AS s FROM t_variant_norm);
-- The two forms unify under UNION ALL and merge together.
SELECT 'unify', sumMerge(s) FROM
(
    SELECT CAST(sumState(v), 'AggregateFunction(sum, Variant(Int64, Float64))') AS s FROM t_variant_norm
    UNION ALL
    SELECT sumState(v) AS s FROM t_variant_norm
);
DROP TABLE t_variant_norm;
