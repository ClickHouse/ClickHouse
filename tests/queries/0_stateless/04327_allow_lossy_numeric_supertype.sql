SET use_variant_as_common_type = 1;

-- Conditions are materialized so the branch types are always combined: a literal-constant
-- condition is folded to the taken branch by the old analyzer (before type-combining), which
-- would hide the Variant/supertype the feature is about.

-- Default (allow_lossy_numeric_supertype = 0): mixed numeric branches with no lossless
-- common type produce a Variant, which value-combining aggregates reject.
SET allow_lossy_numeric_supertype = 0;
SELECT toTypeName(if(materialize(1), toDecimal64(1, 2), 0.));
SELECT toTypeName(multiIf(materialize(1), toInt64(1), 0.));
SELECT toTypeName(multiIf(materialize(toUInt8(0)), NULL, materialize(toUInt8(1)), toDecimal64(1, 2), 0.));
SELECT toTypeName([toDecimal64(1, 2), 0.]);
-- A nullable first argument keeps the second branch, so coalesce/ifNull actually compute a
-- common type (a non-nullable first argument would short-circuit losslessly to Decimal).
SELECT toTypeName(coalesce(materialize(toNullable(toDecimal64(1, 2))), 0.));
SELECT toTypeName(ifNull(materialize(toNullable(toDecimal64(1, 2))), 0.));
SELECT toTypeName(map('a', toDecimal64(1, 2), 'b', 0.));
-- A Map key cannot be Nullable; the Variant builder drops the Nullable wrapper, so a nullable
-- numeric key resolves to a valid (non-nullable) Variant key.
SELECT toTypeName(map(materialize(toNullable(toDecimal64(1, 2))), 1, 0., 2));
SELECT sum(if(materialize(1), toDecimal64(1, 2), 0.)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT avg(multiIf(materialize(1), toInt64(1), 0.)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT max(if(materialize(1), toInt64(1), 0.)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Enabled: the same expressions resolve to a numeric supertype (Float64), so they aggregate.
-- Each resolver threaded by the PR (if/multiIf/coalesce/ifNull/array/map) is exercised.
SET allow_lossy_numeric_supertype = 1;
SELECT toTypeName(if(materialize(1), toDecimal64(1, 2), 0.));
SELECT toTypeName(multiIf(materialize(1), toInt64(1), 0.));
SELECT toTypeName([toDecimal64(1, 2), 0.]);
SELECT toTypeName(coalesce(materialize(toNullable(toDecimal64(1, 2))), 0.));
SELECT toTypeName(ifNull(materialize(toNullable(toDecimal64(1, 2))), 0.));
SELECT toTypeName(map('a', toDecimal64(1, 2), 'b', 0.));
-- NULL-only branches are dropped while nullability is tracked, so a normal conditional
-- shape with a NULL branch resolves to Nullable(Float64) instead of falling to a Variant.
SELECT toTypeName(multiIf(materialize(toUInt8(0)), NULL, materialize(toUInt8(1)), toDecimal64(1, 2), 0.));
-- A Map key cannot be Nullable, so the lossy fallback (which preserves nullability) is not
-- applied to a nullable numeric key: it keeps the valid Variant key instead of throwing. A
-- non-nullable numeric key still resolves to the lossy Float64 supertype.
SELECT toTypeName(map(materialize(toNullable(toDecimal64(1, 2))), 1, 0., 2));
SELECT map(materialize(toNullable(toDecimal64(1, 2))), 1, 0., 2);
SELECT toTypeName(map(toDecimal64(1, 2), 'x', 0., 'y'));

-- End-to-end values: the resolved numeric supertype actually aggregates.
SELECT sum(if(materialize(1), toDecimal64(1, 2), 0.));
SELECT avg(multiIf(materialize(1), toInt64(1), 0.));
SELECT min(if(number % 2, toDecimal64(number, 2), 0.5)), max(if(number % 2, toDecimal64(number, 2), 0.5)) FROM numbers(4);
SELECT sum(coalesce(materialize(toNullable(toDecimal64(number, 2))), 0.)) FROM numbers(4);
SELECT sum(ifNull(if(number % 2, toNullable(toDecimal64(number, 2)), NULL), 0.)) FROM numbers(4);
SELECT sum(arraySum([toDecimal64(number, 2), 0.5])) FROM numbers(4);
SELECT sum(multiIf(number % 2, NULL, number = 0, toDecimal64(number, 2), 0.5)) FROM numbers(4);

-- Integer-only mixes have no floating-point branch, so there is no lossy numeric supertype:
-- they stay a Variant even with the setting on, and aggregates still reject (the setting
-- cannot help, so the error hint must not suggest it).
SELECT toTypeName(if(materialize(toUInt8(1)), toInt64(1), toUInt64(2)));
SELECT sum(if(materialize(toUInt8(1)), toInt64(1), toUInt64(2))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Lossless numeric mixes are unaffected (still resolve to the lossless supertype, not Float64).
SELECT toTypeName(if(materialize(1), toInt32(1), toInt8(2)));
SELECT toTypeName(if(materialize(1), toUInt8(1), toInt16(2)));

-- Non-numeric branches still become a Variant even when the setting is on.
SELECT toTypeName(if(materialize(1), toInt64(1), 'str'));
SELECT toTypeName([toInt64(1), [1, 2]]);
