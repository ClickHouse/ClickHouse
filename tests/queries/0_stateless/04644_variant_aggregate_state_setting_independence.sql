-- An aggregate-function state type that was already declared must keep resolving regardless of the current value of
-- `allow_lossy_numeric_supertype`. The setting gates only the *inference* of the aggregation type from a Variant
-- column; a declared `AggregateFunction(sum, Variant(Int64, Float64))` names its layout explicitly and was validated
-- when it was declared, so `sumMerge` / `avgMerge` / `CAST ... AS AggregateFunction(...)` over it must not start
-- failing after the setting is turned off, or stored states would become unreadable.

DROP TABLE IF EXISTS t_variant_state_setting;

SET allow_lossy_numeric_supertype = 1;

CREATE TABLE t_variant_state_setting
(
    s AggregateFunction(sum, Variant(Int64, Float64)),
    a AggregateFunction(avg, Variant(Int64, Float64))
) ENGINE = Memory;

INSERT INTO t_variant_state_setting
SELECT sumState(v), avgState(v)
FROM (SELECT CAST(1::Int64 AS Variant(Int64, Float64)) AS v UNION ALL SELECT CAST(2.5::Float64 AS Variant(Int64, Float64)) AS v);

SELECT 'merge with the setting on', sumMerge(s), avgMerge(a) FROM t_variant_state_setting;

-- The same reads with the setting back at its default value.
SET allow_lossy_numeric_supertype = 0;

SELECT 'merge with the setting off', sumMerge(s), avgMerge(a) FROM t_variant_state_setting;
SELECT 'state type', toTypeName(s), toTypeName(a) FROM t_variant_state_setting;
SELECT 'cast to the state type', sumMerge(CAST(s AS AggregateFunction(sum, Variant(Int64, Float64)))) FROM t_variant_state_setting;

-- Declaring such a state type is explicit, so it works with the setting off as well, unlike the inference from a
-- Variant column, which stays gated.
DROP TABLE IF EXISTS t_variant_state_setting_2;
CREATE TABLE t_variant_state_setting_2 (s AggregateFunction(sum, Variant(Int64, Float64))) ENGINE = Memory;
INSERT INTO t_variant_state_setting_2 SELECT s FROM t_variant_state_setting;
SELECT 'declared with the setting off', sumMerge(s) FROM t_variant_state_setting_2;
SELECT sum(CAST(1::Int64 AS Variant(Int64, Float64))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_variant_state_setting_2;
DROP TABLE t_variant_state_setting;

-- An invalid *parameter* of an aggregate function must be reported as such even when the argument types had to be
-- adapted from a Variant first: the adapter retries only on "unsupported argument type" errors, and a bad parameter
-- is not one of them.
SET allow_lossy_numeric_supertype = 1;
SELECT kolmogorovSmirnovTest('bogus')(CAST(1::Int64 AS Variant(Int64, Float64)), 0); -- { serverError BAD_ARGUMENTS }
SELECT kolmogorovSmirnovTest('two-sided', 'bogus')(CAST(1::Int64 AS Variant(Int64, Float64)), 0); -- { serverError BAD_ARGUMENTS }
SELECT meanZTest(-1., 0.5, 0.95)(CAST(1::Int64 AS Variant(Int64, Float64)), 0); -- { serverError BAD_ARGUMENTS }
SELECT welchTTest(1.5)(CAST(1::Int64 AS Variant(Int64, Float64)), 0); -- { serverError BAD_ARGUMENTS }
-- The same parameter errors without any Variant, for comparison.
SELECT kolmogorovSmirnovTest('bogus')(1, 0); -- { serverError BAD_ARGUMENTS }
SELECT meanZTest(-1., 0.5, 0.95)(1, 0); -- { serverError BAD_ARGUMENTS }
SELECT welchTTest(1.5)(1, 0); -- { serverError BAD_ARGUMENTS }

-- Argument types that no adaptation can fix are still reported as an illegal type. The statistical aggregates that
-- used to reject argument types with `BAD_ARGUMENTS` now use `ILLEGAL_TYPE_OF_ARGUMENT` like every other function,
-- so that `BAD_ARGUMENTS` is left to mean a genuine semantic error such as the bad parameters above.
SELECT analysisOfVariance(CAST('x'::String AS Variant(String, Array(UInt8))), 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT analysisOfVariance('x', 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT welchTTest('x', 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT studentTTest('x', 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT meanZTest(1., 0.5, 0.95)('x', 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
