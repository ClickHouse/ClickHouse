-- invalid start offset with RANGE
SELECT count() OVER (ORDER BY 3.4028234663852886e38 RANGE BETWEEN 0.0 PRECEDING AND UNBOUNDED FOLLOWING);
SELECT count() OVER (ORDER BY 3.4028234663852886e38 RANGE BETWEEN nan PRECEDING AND UNBOUNDED FOLLOWING); -- { serverError BAD_ARGUMENTS }
-- invalid end offset with RANGE
SELECT count() OVER (ORDER BY 3.4028234663852886e38 RANGE BETWEEN UNBOUNDED PRECEDING AND 0.0 FOLLOWING);
SELECT count() OVER (ORDER BY 3.4028234663852886e38 RANGE BETWEEN UNBOUNDED PRECEDING AND nan FOLLOWING); -- { serverError BAD_ARGUMENTS }
-- a `RANGE` offset that is not a nonnegative number is rejected before execution,
-- with either analyzer, and before a lossy coercion could hide it
SELECT count() OVER (ORDER BY materialize(1.5)::Nullable(Float64) RANGE NULL::Nullable(Float64) PRECEDING); -- { serverError BAD_ARGUMENTS }
SELECT count() OVER (ORDER BY 1.5 RANGE '0.5' PRECEDING) SETTINGS enable_analyzer = 0; -- { serverError BAD_ARGUMENTS }
SELECT count() OVER (ORDER BY 1.0::Decimal32(1) RANGE -0.04::Decimal32(2) PRECEDING); -- { serverError BAD_ARGUMENTS }
