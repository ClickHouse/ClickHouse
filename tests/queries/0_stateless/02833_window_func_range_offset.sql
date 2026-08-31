-- invalid start offset with RANGE
SELECT count() OVER (ORDER BY 3.4028234663852886e38 RANGE BETWEEN 0.0 PRECEDING AND UNBOUNDED FOLLOWING);
SELECT count() OVER (ORDER BY 3.4028234663852886e38 RANGE BETWEEN nan PRECEDING AND UNBOUNDED FOLLOWING); -- { serverError BAD_ARGUMENTS }
SELECT count() OVER (ORDER BY 3.4028234663852886e38 RANGE BETWEEN inf PRECEDING AND UNBOUNDED FOLLOWING); -- { serverError BAD_ARGUMENTS }
-- invalid end offset with RANGE
SELECT count() OVER (ORDER BY 3.4028234663852886e38 RANGE BETWEEN UNBOUNDED PRECEDING AND 0.0 FOLLOWING);
SELECT count() OVER (ORDER BY 3.4028234663852886e38 RANGE BETWEEN UNBOUNDED PRECEDING AND nan FOLLOWING); -- { serverError BAD_ARGUMENTS }
SELECT count() OVER (ORDER BY 3.4028234663852886e38 RANGE BETWEEN UNBOUNDED PRECEDING AND inf FOLLOWING); -- { serverError BAD_ARGUMENTS }
-- the `ORDER BY` column above is floating; these pin that a non-finite offset is rejected for
-- an integer or `Decimal` one too, where coercion would otherwise report it as out of range
SELECT count() OVER (ORDER BY materialize(1)::UInt64 RANGE nan PRECEDING); -- { serverError BAD_ARGUMENTS }
SELECT count() OVER (ORDER BY materialize(1)::UInt64 RANGE BETWEEN CURRENT ROW AND inf FOLLOWING); -- { serverError BAD_ARGUMENTS }
SELECT count() OVER (ORDER BY 1.0::Decimal32(1) RANGE inf PRECEDING); -- { serverError BAD_ARGUMENTS }
SELECT count() OVER (ORDER BY 1.0::Decimal32(1) RANGE BETWEEN CURRENT ROW AND nan FOLLOWING); -- { serverError BAD_ARGUMENTS }
-- a `RANGE` offset that is not a nonnegative number is rejected before execution,
-- with either analyzer, and before a lossy coercion could hide it
SELECT count() OVER (ORDER BY materialize(1.5)::Nullable(Float64) RANGE NULL::Nullable(Float64) PRECEDING); -- { serverError BAD_ARGUMENTS }
SELECT count() OVER (ORDER BY 1.0::Decimal32(1) RANGE -0.04::Decimal32(2) PRECEDING); -- { serverError BAD_ARGUMENTS }
-- the legacy analyzer checks the offset type nowhere else, so these pin that allowing fractional
-- offsets did not start accepting anything it rejected before
SELECT count() OVER (ORDER BY 1.5 RANGE '0.5' PRECEDING) SETTINGS enable_analyzer = 0; -- { serverError BAD_ARGUMENTS }
SELECT count() OVER (ORDER BY 1.5 RANGE true PRECEDING) SETTINGS enable_analyzer = 0; -- { serverError BAD_ARGUMENTS }
SELECT count() OVER (ORDER BY 1.0::Decimal32(1) RANGE toDateTime64('1970-01-01 00:00:01.5', 3) PRECEDING) SETTINGS enable_analyzer = 0; -- { serverError BAD_ARGUMENTS }
