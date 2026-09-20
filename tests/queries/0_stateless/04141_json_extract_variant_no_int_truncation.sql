-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103617
--
-- Before the fix, `JSONExtract` into a `Variant` that contained both an integer
-- and a floating-point member silently truncated fractional JSON numbers: the
-- integer member won and the fractional part was lost.
--
-- After the fix, `VariantNode` first tries every variant with strict semantics
-- (no truncating doubles to integers) so the float/decimal member claims the
-- value losslessly. Only when no float/decimal member can hold the value does
-- the loop fall back to the legacy lenient pass.

-- 1. Mixed-numeric variants must pick the float member for fractional JSON doubles.
SELECT 'mixed_variant_fractional_double';
SELECT
    JSONExtract('{"x": 3.14}', 'x', 'Variant(String, Int64, Float64)') AS v,
    variantType(v) AS active,
    variantElement(v, 'Float64') AS f64_slot,
    variantElement(v, 'Int64') AS i64_slot;

-- 2. Declaration order does not matter: float still wins for fractional values.
SELECT 'declaration_order_does_not_matter';
SELECT
    JSONExtract('{"x": 3.14}', 'x', 'Variant(Int64, Float64)') AS v1,
    variantType(v1),
    JSONExtract('{"x": 3.14}', 'x', 'Variant(Float64, Int64)') AS v2,
    variantType(v2);

-- 3. Negative fractional values pick the float member.
SELECT 'negative_fractional';
SELECT
    JSONExtract('{"x": -2.5}', 'x', 'Variant(String, Int64, Float64)') AS v,
    variantType(v) AS active,
    variantElement(v, 'Float64') AS f64;

-- 4. Very small fractional values pick the float member.
SELECT 'small_fractional';
SELECT
    JSONExtract('{"x": 0.0001}', 'x', 'Variant(Int64, Float64)') AS v,
    variantType(v),
    variantElement(v, 'Float64');

-- 5. JSON integer literals still pick the integer member.
SELECT 'integer_literal_picks_int';
SELECT
    JSONExtract('{"x": 3}', 'x', 'Variant(String, Int64, Float64)') AS v,
    variantType(v) AS active,
    variantElement(v, 'Int64') AS i64;

-- 6. Big fractional numbers that overflow Int64 pick the float member (worked already, kept as smoke).
SELECT 'overflow_picks_float';
SELECT
    JSONExtract('{"x": 1e100}', 'x', 'Variant(String, Int64, Float64)') AS v,
    variantType(v);

-- 7. STRING containing a fractional number must NOT truncate to Int64. Float64 wins
--    because variant priority is Int64 > Float64 > String; with the fix Int64 rejects
--    the non-integral string-as-float and Float64 picks it up.
SELECT 'string_with_fractional_no_int_truncation';
SELECT
    JSONExtract('{"x": "3.14"}', 'x', 'Variant(String, Int64, Float64)') AS v,
    variantType(v) AS active,
    variantElement(v, 'String') AS s,
    variantElement(v, 'Int64') AS i64;

-- 8. STRING containing a fractional number with no String variant: must pick Float64, not Int64.
SELECT 'string_with_fractional_picks_float';
SELECT
    JSONExtract('{"x": "3.14"}', 'x', 'Variant(Int64, Float64)') AS v,
    variantType(v),
    variantElement(v, 'Float64');

-- 9. Decimal members claim fractional values too.
SELECT 'decimal_picks_fractional';
SELECT
    JSONExtract('{"x": 1.5}', 'x', 'Variant(Int64, Decimal(10, 4))') AS v,
    variantType(v),
    variantElement(v, 'Decimal(10, 4)') AS d;

-- 10. Backward compat: when only integer members exist, fractional doubles still truncate (legacy pass).
SELECT 'only_int_variants_fall_back_to_truncation';
SELECT
    JSONExtract('{"x": 3.14}', 'x', 'Variant(Int64, Int32)') AS v,
    variantType(v) AS active,
    variantElement(v, 'Int64') AS i64,
    variantElement(v, 'Int32') AS i32;

-- 11. Backward compat: direct integer extraction still truncates (this fix is Variant-scoped).
SELECT 'direct_int_extract_unchanged';
SELECT JSONExtract('{"x": 3.14}', 'x', 'Int64') AS v;
SELECT JSONExtractInt('{"x": 3.14}', 'x') AS v;

-- 12. Backward compat: direct float extraction unchanged.
SELECT 'direct_float_extract_unchanged';
SELECT JSONExtract('{"x": 3.14}', 'x', 'Float64') AS v;

-- 13. Big integer types that span multiple Variant slots fall through to Float64.
SELECT 'big_int_variants_fractional';
SELECT
    JSONExtract('{"x": 7.5}', 'x', 'Variant(Int64, Int128, Float64)') AS v,
    variantType(v),
    variantElement(v, 'Float64') AS f64;

-- 14. Bool JSON values continue to pick Bool variants when present.
SELECT 'bool_picks_bool';
SELECT
    JSONExtract('{"x": true}', 'x', 'Variant(Bool, Int64)') AS v,
    variantType(v);

-- 15. Closes #103617: original reproducer from the issue.
SELECT 'closes_103617';
SELECT
    JSONExtract('{"x": 3.14}', 'x', 'Variant(String, Int64, Float64)') AS v,
    variantType(v) AS active,
    variantElement(v, 'Float64') AS f64_slot,
    variantElement(v, 'Int64') AS i64_slot
FORMAT Vertical;

-- 16. Intentional broader scope (raised by the automated reviewer on PR #103620):
--     `Variant(String, IntT)` with a fractional JSON DOUBLE now picks `String` instead
--     of silently truncating to `IntT`.
--
--     Previously the lone lenient pass let `Int64` accept `3.14` and the result was
--     `Int64 = 3` (silent data loss). With the new strict-first pass, `Int64` rejects
--     non-integral doubles, the strict pass falls through to `String`, and the JSON
--     element is serialized losslessly as `"3.14"`.
--
--     This is intentional and consistent with the bug being fixed: the original bug is
--     *silent data loss*, and capturing the value as `String` is strictly better than
--     dropping the fractional part. Users who want the integer slot to claim fractional
--     numbers must remove `String` from the variant (test 10 covers `Variant(IntT, IntT)`,
--     which still falls back to truncation via the legacy lenient pass).
SELECT 'string_int_no_float_picks_string';
SELECT
    JSONExtract('{"x": 3.14}', 'x', 'Variant(String, Int64)') AS v,
    variantType(v) AS active,
    variantElement(v, 'String') AS s,
    variantElement(v, 'Int64') AS i64;

-- 17. Same as 16 with a narrower integer width — verifies the rule applies uniformly
--     across all integer types in the strict pass.
SELECT 'string_smaller_int_no_float_picks_string';
SELECT
    JSONExtract('{"x": 3.14}', 'x', 'Variant(String, Int32)') AS v,
    variantType(v) AS active,
    variantElement(v, 'String') AS s,
    variantElement(v, 'Int32') AS i32;

-- 18. Backward compat: integer JSON literals still pick the integer member even when a
--     `String` fallback exists, because the strict pass accepts `Int64` for an exactly
--     representable value before reaching `String`.
SELECT 'string_int_no_float_integer_literal_picks_int';
SELECT
    JSONExtract('{"x": 3}', 'x', 'Variant(String, Int64)') AS v,
    variantType(v) AS active,
    variantElement(v, 'Int64') AS i64;

-- 19. Backward compat: when only String + multiple integer members exist, fractional
--     doubles still pick `String` (not any integer). Confirms the strict pass uniformly
--     rejects all integer members for non-integral doubles, regardless of width.
SELECT 'string_with_multiple_ints_picks_string';
SELECT
    JSONExtract('{"x": 3.14}', 'x', 'Variant(String, Int32, Int64, Int128)') AS v,
    variantType(v);
