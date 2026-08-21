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
-- A nullable numeric Map key resolves to a (non-nullable) Variant key: the Variant builder drops Nullable.
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
-- A non-nullable numeric Map key resolves to the lossy Float64 supertype.
SELECT toTypeName(map(toDecimal64(1, 2), 'x', 0., 'y'));
-- A nullable numeric Map key resolves to Nullable(Float64), which is an invalid Map key, so it throws.
SELECT toTypeName(map(materialize(toNullable(toDecimal64(1, 2))), 1, 0., 2)); -- { serverError BAD_ARGUMENTS }

-- LowCardinality is preserved for functions that keep it (array/map): all-LowCardinality numeric
-- branches keep LowCardinality on the lossy Float64 result, a mix of LowCardinality and plain drops it,
-- and LowCardinality(Nullable) yields LowCardinality(Nullable(Float64)). This mirrors getLeastSupertype
-- so array/map element metadata is not silently corrupted. (Decimal cannot be LowCardinality, so the
-- lossy LowCardinality path uses Int64 + Float64, which also has no lossless common type.)
SELECT toTypeName([toLowCardinality(toInt64(1)), toLowCardinality(0.)]);
SELECT toTypeName([toLowCardinality(toInt64(1)), 0.]);
SELECT toTypeName([toLowCardinality(toNullable(toInt64(1))), toLowCardinality(toNullable(0.))]);
SELECT toTypeName(map('a', toLowCardinality(toInt64(1)), 'b', toLowCardinality(0.)));
-- A bare NULL counts as a non-LowCardinality branch (the regular resolver's LowCardinality phase
-- runs before its Nullable phase), so it drops the wrapper: Array(Nullable(Float64)), not
-- Array(LowCardinality(Nullable(Float64))).
SELECT toTypeName([toLowCardinality(toInt64(1)), NULL, toLowCardinality(0.)]);
SELECT toTypeName(map('a', toLowCardinality(toInt64(1)), 'b', NULL, 'c', toLowCardinality(0.)));
-- if/multiIf strip LowCardinality before combining branch types (their lossless path does the same),
-- so the lossy result is plain Float64 here, matching the existing conditional behavior.
SELECT toTypeName(if(materialize(1), toLowCardinality(toInt64(1)), toLowCardinality(0.)));

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

-- The setting covers the searched form `CASE WHEN ... END` (which goes through multiIf), so
-- it resolves to the numeric supertype like the other conditionals.
SELECT toTypeName(CASE WHEN materialize(1) THEN toDecimal64(1, 2) ELSE 0. END);
-- But the expression form `CASE expr WHEN ... END` (function caseWithExpression) is outside the
-- setting: it computes its result type with plain getLeastSupertype, so mixed numeric branches
-- with no lossless common type still throw NO_COMMON_TYPE regardless of the setting.
SELECT toTypeName(CASE materialize(1) WHEN 1 THEN toDecimal64(1, 2) ELSE 0. END); -- { serverError NO_COMMON_TYPE }
SELECT sum(CASE materialize(1) WHEN 1 THEN toDecimal64(1, 2) ELSE 0. END); -- { serverError NO_COMMON_TYPE }

-- The setting is independent of use_variant_as_common_type: with use_variant_as_common_type = 0
-- the lossy numeric supertype still applies (previously these threw NO_COMMON_TYPE because the
-- lossy fallback was reachable only through the Variant path).
SET use_variant_as_common_type = 0;
SET allow_lossy_numeric_supertype = 1;
SELECT toTypeName(if(materialize(1), toDecimal64(1, 2), 0.));
SELECT toTypeName(multiIf(materialize(1), toInt64(1), 0.));
SELECT toTypeName([toDecimal64(1, 2), 0.]);
SELECT toTypeName(coalesce(materialize(toNullable(toDecimal64(1, 2))), 0.));
SELECT toTypeName(ifNull(materialize(toNullable(toDecimal64(1, 2))), 0.));
SELECT toTypeName(map('a', toDecimal64(1, 2), 'b', 0.));
SELECT sum(if(materialize(1), toDecimal64(1, 2), 0.));
SELECT avg(multiIf(materialize(1), toInt64(1), 0.));
-- When the lossy fallback does not apply (non-numeric branch, or integer-only with no float), a
-- Variant is not produced under use_variant_as_common_type = 0: it throws NO_COMMON_TYPE as before.
SELECT toTypeName(if(materialize(1), toInt64(1), 'str')); -- { serverError NO_COMMON_TYPE }
SELECT toTypeName(if(materialize(toUInt8(1)), toInt64(1), toUInt64(2))); -- { serverError NO_COMMON_TYPE }
-- And with the setting off, the mixed numeric branches still throw.
SET allow_lossy_numeric_supertype = 0;
SELECT toTypeName(if(materialize(1), toDecimal64(1, 2), 0.)); -- { serverError NO_COMMON_TYPE }
