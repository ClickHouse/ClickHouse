SET use_variant_as_common_type = 0;

SELECT ifNotFinite(round(1 / number, 2), 111) FROM numbers(10);

SELECT ifNotFinite(1, 2);
SELECT ifNotFinite(-1.0, 2);
SELECT ifNotFinite(nan, 2);
SELECT ifNotFinite(-1 / 0, 2);
SELECT ifNotFinite(log(0), NULL);
SELECT ifNotFinite(sqrt(-1), -42);
SELECT ifNotFinite(12345678901234567890, -12345678901234567890) SETTINGS enable_context_typed_number_literals = 0; -- { serverError NO_COMMON_TYPE }
-- With context typed literals `-12345678901234567890` is an `Int128`, which has a common type with `UInt64`, so the mismatch needs explicit types.
SELECT ifNotFinite(toUInt64(12345678901234567890), toFloat64(-12345678901234567890)); -- { serverError NO_COMMON_TYPE }

SELECT ifNotFinite(NULL, 1);
