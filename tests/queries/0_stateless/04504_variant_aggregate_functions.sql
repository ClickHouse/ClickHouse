-- Aggregate functions that do not support the Variant type natively (sum, avg, min, max, ...) can now be applied
-- to Variant arguments: they aggregate over the least common supertype of the variants, wrapped in Nullable.
-- A numeric mix with no lossless common supertype (e.g. Decimal + Float64, Int64 + Float64) is aggregated in
-- Float64 when the lossy numeric supertype is enabled with allow_lossy_numeric_supertype -- and only for the
-- arithmetic functions whose result is a floating-point value (sum, avg, ...), exactly as arithmetic does.
-- Exact/order-based functions (min, max, argMin, argMax, ...) keep the original error in that case even under the
-- setting, because a lossy Float64 cast would silently return wrong results (distinct integers above 2^53 collapse).

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;
SET allow_suspicious_types_in_group_by = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS t_variant_agg;
CREATE TABLE t_variant_agg (id UInt64, v Variant(Decimal(7, 2), Float64)) ENGINE = Memory;
INSERT INTO t_variant_agg SELECT number, number::Decimal(7, 2) FROM numbers(3);        -- 0, 1, 2
INSERT INTO t_variant_agg SELECT number + 10, (number + 0.5)::Float64 FROM numbers(3);  -- 0.5, 1.5, 2.5
INSERT INTO t_variant_agg VALUES (100, NULL);

-- With the default allow_lossy_numeric_supertype = 0 the lossy Float64 promotion is off: a numeric mix with no
-- lossless common supertype is rejected with the original error (which points at the setting), for every function.
SET allow_lossy_numeric_supertype = 0;
SELECT sum(v) FROM t_variant_agg; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT avg(v) FROM t_variant_agg; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SET allow_lossy_numeric_supertype = 1;

-- The motivating example: sum over Variant(Decimal(7, 2), Float64) is aggregated over the supertype Float64.
SELECT 'sum', sum(v), toTypeName(sum(v)) FROM t_variant_agg;
SELECT 'avg', avg(v), toTypeName(avg(v)) FROM t_variant_agg;
SELECT 'sumKahan', sumKahan(v) FROM t_variant_agg;

-- Exact/order-based functions have no lossless common supertype here (Decimal + Float64), so the Float64 fallback
-- is not applied to them and the original error is reported (sumWithOverflow is exact-integer, not float-promoting).
SELECT min(v) FROM t_variant_agg; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT max(v) FROM t_variant_agg; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT sumWithOverflow(v) FROM t_variant_agg; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Combinators are applied around the adapter (the adapter is the outermost wrapper).
SELECT 'sumIf', sumIf(v, id < 10) FROM t_variant_agg;
SELECT 'sumState/Merge', sumMerge(s) FROM (SELECT sumState(v) AS s FROM t_variant_agg);

-- GROUP BY.
SELECT 'groupBy', id < 10 AS g, sum(v) FROM t_variant_agg GROUP BY g ORDER BY g;

DROP TABLE t_variant_agg;

-- A clean common supertype is preserved (no Float64 fallback): Variant(UInt8, UInt32) -> UInt32 -> sum -> UInt64,
-- and min/max over such a Variant work through the adapter (the supertype is lossless, so values are exact).
DROP TABLE IF EXISTS t_variant_uint;
CREATE TABLE t_variant_uint (v Variant(UInt8, UInt32)) ENGINE = Memory;
INSERT INTO t_variant_uint SELECT number::UInt8 FROM numbers(3);
INSERT INTO t_variant_uint SELECT (number + 1000)::UInt32 FROM numbers(3);
SELECT 'uint', sum(v), toTypeName(sum(v)) FROM t_variant_uint;
SELECT 'uint min/max', min(v), max(v), toTypeName(min(v)) FROM t_variant_uint;
DROP TABLE t_variant_uint;

-- min/max also work when the lossless common supertype is a floating-point type: Variant(Int32, Float64) -> Float64
-- (Int32 is exactly representable in Float64), so, unlike the Int64 + Float64 case below, the values are preserved.
DROP TABLE IF EXISTS t_variant_i32f64;
CREATE TABLE t_variant_i32f64 (v Variant(Int32, Float64)) ENGINE = Memory;
INSERT INTO t_variant_i32f64 VALUES (7), (2.5), (-3), (NULL);
SELECT 'i32f64 min/max', min(v), max(v), toTypeName(min(v)) FROM t_variant_i32f64;
DROP TABLE t_variant_i32f64;

-- All-NULL and empty aggregation (arithmetic functions; the numeric mix Int64 + Float64 falls back to Float64).
DROP TABLE IF EXISTS t_variant_null;
CREATE TABLE t_variant_null (v Variant(Int64, Float64)) ENGINE = Memory;
INSERT INTO t_variant_null VALUES (NULL), (NULL);
SELECT 'all null', sum(v), avg(v) FROM t_variant_null;
SELECT 'empty', sum(v) FROM t_variant_null WHERE 0;
-- Exact/order-based functions over the same lossy numeric mix (Int64 + Float64) report the original error.
SELECT min(v) FROM t_variant_null; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT max(v) FROM t_variant_null; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMax(v, v) FROM t_variant_null; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE t_variant_null;

-- Functions that already support Variant natively are not adapted (the result type keeps the Variant), and the
-- native resolution preserves the standard NULL-skipping contract (AggregateFunctionVariantNull): the NULL row
-- is not counted by uniqExact, exactly as a NULL of a Nullable argument would not be.
DROP TABLE IF EXISTS t_variant_native;
CREATE TABLE t_variant_native (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO t_variant_native VALUES (1), ('a'), (2), (NULL);
SELECT 'native', count(v), toTypeName(any(v)), uniqExact(v) FROM t_variant_native;

-- Variants without a common numeric supertype are still rejected with the original error.
SELECT sum(v) FROM t_variant_native; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT avg(v) FROM t_variant_native; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE t_variant_native;

-- A top-level Variant whose common supertype is a container type (Array/Tuple/Map) is out of scope: the adapter
-- casts to Nullable(supertype) to carry the Variant's implicit NULLs, and such types cannot be wrapped in Nullable.
-- The supertype Array(UInt16) is orderable, so min/max over it are legal, but the Variant is still rejected with
-- the original error (supporting it would require tracking the Variant NULLs separately -- a natural follow-up).
SELECT min(CAST([1] AS Variant(Array(UInt8), Array(UInt16)))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT max(CAST([1] AS Variant(Array(UInt8), Array(UInt16)))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Only top-level Variant arguments are adapted. A combinator that merely exposes a nested Variant from ordinary
-- user data does not get the adapter, so a nested Variant argument stays out of scope: e.g. -Array turns
-- sumArray(Array(Variant(...))) into a nested sum(Variant(...)), which is still rejected with the original error.
-- (The adapter is only reintroduced for a Variant that comes from a stored aggregate-function state type, as -Merge
-- does below.)
SELECT sumArray([CAST(1 AS Variant(UInt8, UInt64)), CAST(2 AS Variant(UInt8, UInt64))]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT minArray([CAST(1 AS Variant(UInt8, UInt64)), CAST(2 AS Variant(UInt8, UInt64))]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The -Tuple combinator (the only combinator that wraps one nested function per argument list, e.g. one per tuple
-- element) resolves each element's function without the adapter too, so a nested Variant inside a tuple element also
-- stays out of scope: sumTuple(tuple(Variant(...))) is rejected with the original error, just like the -Array case
-- above. Ordinary (non-Variant) tuples are unaffected.
SELECT 'sumTuple', sumTuple(tuple(number, number + 1)) FROM numbers(4);
SELECT sumTuple(tuple(CAST(1 AS Variant(UInt8, UInt64)))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT avgTuple(tuple(CAST(1 AS Variant(UInt8, UInt64)))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The -Merge combinator over an AggregateFunction(f, Variant(...)) state type. Such a state type is constructible
-- (e.g. as the type of a non-final aggregation block, which keeps the original Variant argument list) and
-- reconstructible, so -Merge must resolve its nested function through the same Variant adapter, otherwise it would
-- throw ILLEGAL_TYPE_OF_ARGUMENT when reconstructing the state. sum/avg over Variant(Int64, Float64) use the
-- Float64 fallback (they are float-promoting and allow_lossy_numeric_supertype is enabled in this session), so
-- such state types can be declared. Background reconstruction of an already-declared state type (table load /
-- ATTACH at startup, merges) has no query context and always allows the promotion, so a table created this way
-- never becomes unloadable because of the setting's current value.
DROP TABLE IF EXISTS t_variant_state;
CREATE TABLE t_variant_state
(
    s AggregateFunction(sum, Variant(Int64, Float64)),
    a AggregateFunction(avg, Variant(Int64, Float64))
) ENGINE = Memory;
SELECT 'merge empty', sumMerge(s), avgMerge(a) FROM t_variant_state;
SELECT 'merge types', toTypeName(sumMerge(s)), toTypeName(avgMerge(a)) FROM t_variant_state;
DROP TABLE t_variant_state;

-- min/max state types over a Variant with a lossless common supertype work through the adapter too.
DROP TABLE IF EXISTS t_variant_state_minmax;
CREATE TABLE t_variant_state_minmax
(
    mn AggregateFunction(min, Variant(UInt8, UInt32)),
    mx AggregateFunction(max, Variant(UInt8, UInt32))
) ENGINE = Memory;
SELECT 'merge minmax', minMerge(mn), maxMerge(mx), toTypeName(minMerge(mn)) FROM t_variant_state_minmax;
DROP TABLE t_variant_state_minmax;

-- An exact/order-based state over a Variant with no lossless common supertype cannot be declared: resolving min
-- over Variant(Int64, Float64) reports the original error (only float-promoting functions use the Float64 fallback).
CREATE TABLE t_variant_state_minbad (mn AggregateFunction(min, Variant(Int64, Float64))) ENGINE = Memory; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

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

-- argMin/argMax (and the *ArgMin/*ArgMax combinators) natively accept a Variant in the returned "arg" position and
-- reject it only in the comparable "key" position. When both are Variant, the adapter converts only the key: the arg
-- is kept as-is, so the result type stays the original Variant instead of collapsing to Nullable(supertype). Note the
-- arg here -- Variant(String, UInt64) -- has no common supertype and could not be adapted at all, yet the call still
-- resolves because only the key needs adapting (the key Variant(UInt8, UInt64) has the lossless supertype UInt64).
DROP TABLE IF EXISTS t_variant_argminmax;
CREATE TABLE t_variant_argminmax (arg Variant(String, UInt64), key Variant(UInt8, UInt64)) ENGINE = Memory;
INSERT INTO t_variant_argminmax VALUES ('a', 1), ('b', 3), ('c', 2);
SELECT 'argMax', argMax(arg, key) AS r, toTypeName(r) FROM t_variant_argminmax;
SELECT 'argMin', argMin(arg, key) AS r, toTypeName(r) FROM t_variant_argminmax;
DROP TABLE t_variant_argminmax;

-- A Variant in both positions with a numeric supertype: the result type is still the original arg Variant, not the
-- adapted Nullable(supertype) of the key.
SELECT 'argMax type', toTypeName(argMax(CAST(number AS Variant(UInt8, UInt64)), CAST(number AS Variant(UInt8, UInt64)))) FROM numbers(4);

-- Not every aggregate creator rejects an unsupported argument type with ILLEGAL_TYPE_OF_ARGUMENT: rankCorr,
-- mannWhitneyUTest and kolmogorovSmirnovTest reject with NOT_IMPLEMENTED. The adapter retries on both of these
-- "unsupported argument type" errors, so those functions can be applied to a Variant argument too, aggregating
-- over its supertype (here the lossless UInt64). rankCorr over perfectly correlated data is 1; analysisOfVariance
-- returns the same value as the equivalent aggregation over the plain supertype.
SELECT 'rankCorr', rankCorr(CAST(number AS Variant(UInt8, UInt64)), number) FROM numbers(4);
SELECT 'analysisOfVariance', analysisOfVariance(CAST(number AS Variant(UInt8, UInt64)), number % 2) = analysisOfVariance(toUInt64(number), number % 2) FROM numbers(10);

-- When the Variant cannot be adapted (no common numeric supertype), the adapter gives up and the original creator
-- error is reported unchanged -- for rankCorr that is its own NOT_IMPLEMENTED, not ILLEGAL_TYPE_OF_ARGUMENT.
SELECT rankCorr(CAST(toUInt64(1) AS Variant(String, UInt64)), 1); -- { serverError NOT_IMPLEMENTED }
SELECT analysisOfVariance(CAST(toUInt64(1) AS Variant(String, UInt64)), 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The Float64 fallback covers the whole statistical / moment family, not just sum/avg. The tests above only exercise
-- a Variant with a lossless common supertype (Variant(UInt8, UInt64) -> UInt64), which never reaches the fallback.
-- Here the value Variant is Int64 + Float64, which has NO lossless supertype, so these functions -- whose result is a
-- floating-point statistic read from the inputs via Float64 -- must aggregate over Float64, exactly like sum/avg. This
-- is a regression for the review comment that the float-promoting allowlist omitted the numerically stable
-- variance/covariance/correlation siblings, the *TTest family, meanZTest, analysisOfVariance and the rank/matrix
-- statistics. Each result must match the same function applied to the explicit Nullable(Float64) cast of the Variant,
-- which is exactly what the adapter computes internally.
DROP TABLE IF EXISTS t_variant_stat;
CREATE TABLE t_variant_stat (x Variant(Int64, Float64), y Variant(Int64, Float64), g UInt8, grp UInt64) ENGINE = Memory;
INSERT INTO t_variant_stat VALUES
    (1::Int64, 5::Int64, 0, 0),
    (2.5::Float64, 6.5::Float64, 0, 0),
    (3::Int64, 4::Int64, 1, 1),
    (4.5::Float64, 9.5::Float64, 1, 1),
    (2::Int64, 3.5::Float64, 0, 2),
    (NULL, NULL, 1, 2);

SELECT 'varSampStable',         varSampStable(x)                = varSampStable(CAST(x AS Nullable(Float64)))                                              FROM t_variant_stat;
SELECT 'varPopStable',          varPopStable(x)                 = varPopStable(CAST(x AS Nullable(Float64)))                                               FROM t_variant_stat;
SELECT 'stddevSampStable',      stddevSampStable(x)             = stddevSampStable(CAST(x AS Nullable(Float64)))                                           FROM t_variant_stat;
SELECT 'stddevPopStable',       stddevPopStable(x)              = stddevPopStable(CAST(x AS Nullable(Float64)))                                            FROM t_variant_stat;
SELECT 'covarSampStable',       covarSampStable(x, y)           = covarSampStable(CAST(x AS Nullable(Float64)), CAST(y AS Nullable(Float64)))             FROM t_variant_stat;
SELECT 'covarPopStable',        covarPopStable(x, y)            = covarPopStable(CAST(x AS Nullable(Float64)), CAST(y AS Nullable(Float64)))              FROM t_variant_stat;
SELECT 'corrStable',            corrStable(x, y)                = corrStable(CAST(x AS Nullable(Float64)), CAST(y AS Nullable(Float64)))                  FROM t_variant_stat;
SELECT 'covarSampMatrix',       covarSampMatrix(x, y)           = covarSampMatrix(CAST(x AS Nullable(Float64)), CAST(y AS Nullable(Float64)))             FROM t_variant_stat;
SELECT 'covarPopMatrix',        covarPopMatrix(x, y)            = covarPopMatrix(CAST(x AS Nullable(Float64)), CAST(y AS Nullable(Float64)))              FROM t_variant_stat;
SELECT 'corrMatrix',            corrMatrix(x, y)                = corrMatrix(CAST(x AS Nullable(Float64)), CAST(y AS Nullable(Float64)))                  FROM t_variant_stat;
SELECT 'studentTTest',          studentTTest(x, g)              = studentTTest(CAST(x AS Nullable(Float64)), g)                                           FROM t_variant_stat;
SELECT 'welchTTest',            welchTTest(x, g)                = welchTTest(CAST(x AS Nullable(Float64)), g)                                             FROM t_variant_stat;
SELECT 'studentTTestOneSample', studentTTestOneSample(x, 2.0)   = studentTTestOneSample(CAST(x AS Nullable(Float64)), 2.0)                                FROM t_variant_stat;
SELECT 'meanZTest',             meanZTest(0.5, 0.6, 0.95)(x, g) = meanZTest(0.5, 0.6, 0.95)(CAST(x AS Nullable(Float64)), g)                              FROM t_variant_stat;
SELECT 'analysisOfVariance',    analysisOfVariance(x, grp)      = analysisOfVariance(CAST(x AS Nullable(Float64)), grp)                                   FROM t_variant_stat;
SELECT 'rankCorr',              rankCorr(x, y)                  = rankCorr(CAST(x AS Nullable(Float64)), CAST(y AS Nullable(Float64)))                    FROM t_variant_stat;
SELECT 'mannWhitneyUTest',      mannWhitneyUTest(x, g)          = mannWhitneyUTest(CAST(x AS Nullable(Float64)), g)                                       FROM t_variant_stat;
SELECT 'kolmogorovSmirnovTest', kolmogorovSmirnovTest(x, g)     = kolmogorovSmirnovTest(CAST(x AS Nullable(Float64)), g)                                  FROM t_variant_stat;
-- simpleLinearRegression also reads both numeric arguments via getFloat64 and returns Float64 (slope, intercept), so
-- it is float-promoting too: over a numeric mix with no lossless supertype it must aggregate over Float64, matching
-- the explicit Nullable(Float64) cast. Its base name was missing from the allowlist, so this is a regression for it
-- (the gap also leaked into simpleLinearRegressionIf / State / Merge through suffix stripping).
SELECT 'simpleLinearRegression', simpleLinearRegression(x, y)    = simpleLinearRegression(CAST(x AS Nullable(Float64)), CAST(y AS Nullable(Float64)))      FROM t_variant_stat;
-- exponentialMovingAverage, boundingRatio and largestTriangleThreeBuckets also read their numeric inputs via
-- getFloat64 and return Float64 results, so they are float-promoting too: over a numeric mix with no lossless
-- supertype they must aggregate over Float64, matching the explicit Nullable(Float64) cast. Their base names were
-- missing from the allowlist, so these are regressions for them.
SELECT 'exponentialMovingAverage', exponentialMovingAverage(1)(x, y) = exponentialMovingAverage(1)(CAST(x AS Nullable(Float64)), CAST(y AS Nullable(Float64))) FROM t_variant_stat;
SELECT 'boundingRatio',           boundingRatio(x, y)              = boundingRatio(CAST(x AS Nullable(Float64)), CAST(y AS Nullable(Float64)))            FROM t_variant_stat;
SELECT 'largestTriangleThreeBuckets', largestTriangleThreeBuckets(4)(x, y) = largestTriangleThreeBuckets(4)(CAST(x AS Nullable(Float64)), CAST(y AS Nullable(Float64))) FROM t_variant_stat;
-- sumCount is the same arithmetic family as sum/avg (it computes the (sum, count) pair via the same accumulation
-- as avg), so it is float-promoting too: over a numeric mix with no lossless supertype it must aggregate over
-- Float64, matching the explicit Nullable(Float64) cast. Its base name was missing from the allowlist, so this is
-- a regression for it (the gap also leaked into sumCountIf / State / Merge through suffix stripping).
SELECT 'sumCount',              sumCount(x)                     = sumCount(CAST(x AS Nullable(Float64)))                                                  FROM t_variant_stat;
-- The stochastic machine-learning aggregates also read their numeric inputs (the target and every feature) via
-- getFloat64 and return Float64 model weights, so they are float-promoting too: over a numeric mix with no lossless
-- supertype they must train over Float64, matching the explicit Nullable(Float64) cast. Their base names were
-- missing from the allowlist, so these are regressions for them (the gap also leaked into their State / Merge forms
-- through suffix stripping).
SELECT 'stochasticLinearRegression', stochasticLinearRegression(0.1, 0.5, 1, 'SGD')(x, y) = stochasticLinearRegression(0.1, 0.5, 1, 'SGD')(CAST(x AS Nullable(Float64)), CAST(y AS Nullable(Float64))) FROM t_variant_stat;
SELECT 'stochasticLogisticRegression', stochasticLogisticRegression(0.1, 0.5, 1, 'SGD')(g, x) = stochasticLogisticRegression(0.1, 0.5, 1, 'SGD')(g, CAST(x AS Nullable(Float64))) FROM t_variant_stat;

-- Combinators compose with the fallback (the adapter is the outermost wrapper): -If filters rows, and a stored
-- -State round-trips through -Merge, both aggregating over the same Float64 supertype.
SELECT 'studentTTestIf',        studentTTestIf(x, g, grp != 2)  = studentTTestIf(CAST(x AS Nullable(Float64)), g, grp != 2)                               FROM t_variant_stat;
SELECT 'varPopStableState',     varPopStableMerge(vs)           = varPopStableMerge(fs) FROM (SELECT varPopStableState(x) AS vs, varPopStableState(CAST(x AS Nullable(Float64))) AS fs FROM t_variant_stat);

DROP TABLE t_variant_stat;
