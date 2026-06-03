-- Tags: no-fasttest
--
-- Per-branch parity tests for `nativeCastWithDecimalScale`, used by JIT-compiled
-- `if`/`multiIf` when at least one branch has a `Decimal` type.
--
-- Each block prints the same expression twice: once with `compile_expressions = 1`
-- (the JIT path that calls `nativeCastWithDecimalScale`) and once with
-- `compile_expressions = 0` (the interpreter path). Both must produce identical
-- output. The "jit" / "no_jit" label on every row makes a divergence trivially
-- visible in the diff against the reference file.
--
-- IMPORTANT: every case wraps the `if`/`multiIf` condition in `materialize(...)`.
-- Without it, the analyzer constant-folds the entire expression at planning
-- time (a constant condition with constant branches collapses to a single
-- `Const` column) and the `if` function is never invoked, so the JIT path is
-- bypassed. `materialize` on the condition forces the function to be evaluated
-- per row, so `FunctionIfBase::compileImpl` runs and `nativeCastWithDecimalScale`
-- is called for each branch's cast to the result type. Verified via
-- `EXPLAIN actions = 1`: with `materialize` the action graph contains an
-- explicit `FUNCTION if(...)` node; without it, the entire expression collapses
-- to a single `COLUMN Const(...)` produced at planning time.
--
-- Background: regression first reported in
-- https://github.com/ClickHouse/ClickHouse/issues/103808 and fixed in PR #103809
-- ("Fix wrong values from JIT-compiled if/multiIf with Decimal result").
--
-- Branches of `nativeCastWithDecimalScale` exercised here:
--   * Integer -> `Decimal`, `to_scale = 0`            (widen only)
--   * Integer -> `Decimal`, `to_scale > 0`            (widen + multiply by `10^to_scale`)
--     - factors that fit in 64 bits                   (small precision targets)
--     - factors that exceed 64 bits, fit in 128 bits  (`Decimal128` `to_scale >= 20`)
--     - factors that exceed 128 bits                  (`Decimal256` `to_scale >= 39`)
--   * `Decimal` -> `Decimal`, same scale, widen integer storage
--   * Identity (`from_type == to_type`)
--   * `Nullable` wrappers                             (`Nullable -> Nullable`, `T -> Nullable`)
--
-- Branches NOT exercised here, with reasons:
--
--   * `Float32`/`Float64` <-> `Decimal`, `Date`/`Date32` <-> `Decimal`:
--     the analyzer promotes the result type to `Variant` because
--     `use_variant_as_common_type = 1` by default, and `canBeNativeType`
--     excludes `Variant`. `FunctionIfBase::isCompilableImpl` returns `false`
--     and the helper is never called. The helper still has correct code paths
--     for these (the `pow10_fp_const` helper avoids 64-bit narrowing via
--     `APFloat::convertFromAPInt` so high-bit-width `Decimal128`/`Decimal256`
--     factors stay accurate) so it is forward-compatible if the analyzer ever
--     stops promoting to `Variant`.
--
--   * `Decimal` -> `Decimal` with different scales (scale increase OR decrease):
--     `FunctionIf::executeImpl` rejects this with `NOT_IMPLEMENTED:
--     "Conditional functions with different Decimal scales"` at planning time
--     (via `executeDryRunImpl` from `ActionsDAG::updateHeader`). Even with
--     `compile_expressions = 1` the planner runs the interpreter to validate
--     types, so the JIT path is unreachable from `if`/`multiIf` for any
--     two-Decimal pair whose scales differ. The Dec->Dec scale-multiplier and
--     scale-divider paths in `nativeCastWithDecimalScale` are exercised
--     indirectly through the `Integer -> Decimal` arm above, which produces
--     the same `pow10_int_const(width, n)` factor on the same arithmetic IR.
--     The dedicated Dec->Dec scale-conversion case is only reachable through
--     explicit `cast(...)`, which uses different machinery.
--
--   * `Decimal` -> integer (narrowing): unreachable through `if` typing because
--     the supertype always picks the wider type (Decimal). Same situation as
--     above.
--
--   * Integer -> `Decimal` where the integer's digit count exceeds the target
--     precision's headroom: e.g. `if(Int8, Decimal(76, 74))` requires precision
--     >= 3 + 74 = 77 > 76 and the analyzer promotes to `Variant`. We pick
--     scales that leave just enough headroom for the source integer.
--
-- Note on supertype promotion: `if(Int<N>, Decimal(P, S))` picks the least
-- supertype, which often is wider than `P` because the integer needs digits
-- before the decimal point. For example `if(bool, Decimal(38, 30), Int32)`
-- promotes to `Decimal(76, 30)` (Int32 needs 10 digits + scale 30 = 40 > 38),
-- so its multiplier `10^30` actually runs in 256-bit `APInt` arithmetic. The
-- inline comment on each high-scale case names the actual JIT supertype.

SET min_count_to_compile_expression = 0;

-- ============================================================================
-- Branch: Integer -> Decimal, to_scale = 0  (just widen integer storage)
-- ============================================================================
SELECT 'int8_to_dec9_0:jit',     toString(if(materialize(toBool(1)), 1::Decimal(9, 0), toInt8(7))) SETTINGS compile_expressions = 1;
SELECT 'int8_to_dec9_0:no_jit',  toString(if(materialize(toBool(1)), 1::Decimal(9, 0), toInt8(7))) SETTINGS compile_expressions = 0;

SELECT 'uint8_to_dec18_0:jit',     toString(if(materialize(toBool(0)), 1::Decimal(18, 0), toUInt8(255))) SETTINGS compile_expressions = 1;
SELECT 'uint8_to_dec18_0:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(18, 0), toUInt8(255))) SETTINGS compile_expressions = 0;

SELECT 'int64_to_dec38_0:jit',     toString(if(materialize(toBool(0)), 1::Decimal(38, 0), toInt64(-9223372036854775807))) SETTINGS compile_expressions = 1;
SELECT 'int64_to_dec38_0:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(38, 0), toInt64(-9223372036854775807))) SETTINGS compile_expressions = 0;

SELECT 'uint64_to_dec38_0:jit',     toString(if(materialize(toBool(0)), 1::Decimal(38, 0), toUInt64(18446744073709551615))) SETTINGS compile_expressions = 1;
SELECT 'uint64_to_dec38_0:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(38, 0), toUInt64(18446744073709551615))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: Integer -> Decimal, to_scale > 0, factor `10^to_scale` <= 64 bits
-- ============================================================================
SELECT 'int8_to_dec9_4_pos:jit',     toString(if(materialize(toBool(0)), 1::Decimal(9, 4), toInt8(7))) SETTINGS compile_expressions = 1;
SELECT 'int8_to_dec9_4_pos:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(9, 4), toInt8(7))) SETTINGS compile_expressions = 0;

SELECT 'int8_to_dec9_4_neg:jit',     toString(if(materialize(toBool(0)), 1::Decimal(9, 4), toInt8(-7))) SETTINGS compile_expressions = 1;
SELECT 'int8_to_dec9_4_neg:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(9, 4), toInt8(-7))) SETTINGS compile_expressions = 0;

SELECT 'int16_to_dec18_4:jit',     toString(if(materialize(toBool(0)), 1::Decimal(18, 4), toInt16(-32000))) SETTINGS compile_expressions = 1;
SELECT 'int16_to_dec18_4:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(18, 4), toInt16(-32000))) SETTINGS compile_expressions = 0;

SELECT 'uint16_to_dec18_4:jit',     toString(if(materialize(toBool(0)), 1::Decimal(18, 4), toUInt16(65535))) SETTINGS compile_expressions = 1;
SELECT 'uint16_to_dec18_4:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(18, 4), toUInt16(65535))) SETTINGS compile_expressions = 0;

SELECT 'int32_to_dec18_7:jit',     toString(if(materialize(toBool(0)), 1::Decimal(18, 7), toInt32(-1234567890))) SETTINGS compile_expressions = 1;
SELECT 'int32_to_dec18_7:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(18, 7), toInt32(-1234567890))) SETTINGS compile_expressions = 0;

SELECT 'uint32_to_dec18_7:jit',     toString(if(materialize(toBool(0)), 1::Decimal(18, 7), toUInt32(4294967295))) SETTINGS compile_expressions = 1;
SELECT 'uint32_to_dec18_7:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(18, 7), toUInt32(4294967295))) SETTINGS compile_expressions = 0;

-- Decimal128 storage (128-bit), factor `10^to_scale` still <= 64 bits.
SELECT 'int64_to_dec38_11:jit',     toString(if(materialize(toBool(0)), 1::Decimal(38, 11), toInt64(123456789))) SETTINGS compile_expressions = 1;
SELECT 'int64_to_dec38_11:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(38, 11), toInt64(123456789))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: Integer -> Decimal128 (128-bit storage) with `10^to_scale` > 64 bits.
-- The `pow10_int_const` factor is constructed in 128-bit `APInt`. Supertype
-- stays `Decimal(38, S)` because the integer source's digit count fits in
-- 38 - S precision headroom (e.g. Int8 with scale 35: 3 digits + 35 = 38).
-- ============================================================================
SELECT 'int8_to_dec38_35:jit',     toString(if(materialize(toBool(0)), 1::Decimal(38, 35), toInt8(7))) SETTINGS compile_expressions = 1;
SELECT 'int8_to_dec38_35:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(38, 35), toInt8(7))) SETTINGS compile_expressions = 0;

SELECT 'uint8_to_dec38_35:jit',     toString(if(materialize(toBool(0)), 1::Decimal(38, 35), toUInt8(7))) SETTINGS compile_expressions = 1;
SELECT 'uint8_to_dec38_35:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(38, 35), toUInt8(7))) SETTINGS compile_expressions = 0;

SELECT 'int16_to_dec38_33:jit',     toString(if(materialize(toBool(0)), 1::Decimal(38, 33), toInt16(-7))) SETTINGS compile_expressions = 1;
SELECT 'int16_to_dec38_33:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(38, 33), toInt16(-7))) SETTINGS compile_expressions = 0;

SELECT 'int32_to_dec38_28:jit',     toString(if(materialize(toBool(0)), 1::Decimal(38, 28), toInt32(7))) SETTINGS compile_expressions = 1;
SELECT 'int32_to_dec38_28:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(38, 28), toInt32(7))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: Integer -> Decimal256 (256-bit storage) with `10^to_scale` > 128 bits.
-- The `pow10_int_const` factor is constructed in 256-bit `APInt`. Supertype is
-- `Decimal(76, S)` either by direct request or by precision-headroom promotion.
-- ============================================================================
-- `if(bool, Decimal(38, 30), Int32)` promotes to `Decimal(76, 30)` because
-- Int32 needs 10 digits + scale 30 = 40 > 38, exceeding Decimal128 precision.
SELECT 'int32_to_dec38_30_via_dec76:jit',     toString(if(materialize(toBool(0)), 1::Decimal(38, 30), toInt32(-3))) SETTINGS compile_expressions = 1;
SELECT 'int32_to_dec38_30_via_dec76:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(38, 30), toInt32(-3))) SETTINGS compile_expressions = 0;

-- `if(bool, Decimal(38, 37), Int8)` -> `Decimal(76, 37)` for the same reason.
SELECT 'int8_to_dec38_37_via_dec76:jit',     toString(if(materialize(toBool(0)), 1::Decimal(38, 37), toInt8(7))) SETTINGS compile_expressions = 1;
SELECT 'int8_to_dec38_37_via_dec76:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(38, 37), toInt8(7))) SETTINGS compile_expressions = 0;

-- Direct Decimal256 supertype (no promotion), factor `10^50` ~ 167 bits.
SELECT 'int32_to_dec76_50:jit',     toString(if(materialize(toBool(0)), 1::Decimal(76, 50), toInt32(7))) SETTINGS compile_expressions = 1;
SELECT 'int32_to_dec76_50:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(76, 50), toInt32(7))) SETTINGS compile_expressions = 0;

-- Direct Decimal256, factor `10^73` ~ 243 bits. Int8 fits because 3 + 73 = 76.
SELECT 'int8_to_dec76_73:jit',     toString(if(materialize(toBool(0)), 1::Decimal(76, 73), toInt8(-3))) SETTINGS compile_expressions = 1;
SELECT 'int8_to_dec76_73:no_jit',  toString(if(materialize(toBool(0)), 1::Decimal(76, 73), toInt8(-3))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: Decimal -> Decimal, same scale (only widen integer storage type).
-- Different precisions but identical scales pass `FunctionIf::decimalScale`
-- check (it requires `left_scale == right_scale`, not equal precisions).
-- ============================================================================
-- 32-bit -> 64-bit storage transition.
SELECT 'dec9_to_dec18_same_scale:jit',     toString(if(materialize(toBool(1)), 1.5::Decimal(18, 4), 2.5::Decimal(9, 4))) SETTINGS compile_expressions = 1;
SELECT 'dec9_to_dec18_same_scale:no_jit',  toString(if(materialize(toBool(1)), 1.5::Decimal(18, 4), 2.5::Decimal(9, 4))) SETTINGS compile_expressions = 0;

-- 64-bit -> 128-bit storage transition.
SELECT 'dec18_to_dec38_same_scale:jit',     toString(if(materialize(toBool(0)), 1.5::Decimal(38, 4), 2.5::Decimal(18, 4))) SETTINGS compile_expressions = 1;
SELECT 'dec18_to_dec38_same_scale:no_jit',  toString(if(materialize(toBool(0)), 1.5::Decimal(38, 4), 2.5::Decimal(18, 4))) SETTINGS compile_expressions = 0;

-- 128-bit -> 256-bit storage transition.
SELECT 'dec38_to_dec76_same_scale:jit',     toString(if(materialize(toBool(1)), 1.5::Decimal(76, 4), 2.5::Decimal(38, 4))) SETTINGS compile_expressions = 1;
SELECT 'dec38_to_dec76_same_scale:no_jit',  toString(if(materialize(toBool(1)), 1.5::Decimal(76, 4), 2.5::Decimal(38, 4))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: Nullable wrappers.
-- The reachable arms through `if`/`multiIf` are:
--   * `T -> Nullable(T)` (the supertype lifts non-Nullable side to Nullable)
--   * `Nullable(T) -> Nullable(T')` where `T` and `T'` have identical scales
--     (or one side is integer, which goes through the `Decimal <- Int` arm).
-- The pure `Nullable -> non-Nullable` arm is unreachable from `if` typing.
-- ============================================================================
-- Nullable Decimal vs non-Nullable Int -> Nullable Decimal supertype.
-- Exercises `T -> Nullable(T)` arm of `nativeCastWithDecimalScale` (lifts the
-- inner Int -> Decimal cast and then wraps with null = 0 marker).
SELECT 'nullable_dec_int_jit',     toString(if(materialize(toBool(1)), toNullable(1.5::Decimal(38, 30)), toInt32(7))) SETTINGS compile_expressions = 1;
SELECT 'nullable_dec_int_no_jit',  toString(if(materialize(toBool(1)), toNullable(1.5::Decimal(38, 30)), toInt32(7))) SETTINGS compile_expressions = 0;

-- Both sides Nullable, identical scales (different precisions).
-- Exercises `Nullable -> Nullable` arm of `nativeCastWithDecimalScale`.
SELECT 'both_nullable_same_scale:jit',     toString(if(materialize(toBool(0)), toNullable(1.5::Decimal(38, 4)), toNullable(2.5::Decimal(18, 4)))) SETTINGS compile_expressions = 1;
SELECT 'both_nullable_same_scale:no_jit',  toString(if(materialize(toBool(0)), toNullable(1.5::Decimal(38, 4)), toNullable(2.5::Decimal(18, 4)))) SETTINGS compile_expressions = 0;

-- Non-Nullable Decimal vs typed-NULL Nullable Decimal, identical scales.
-- Exercises `T -> Nullable(T)` plus the null-mask propagation for the picked
-- NULL value.
SELECT 'nullable_picks_null_jit',     toString(if(materialize(toBool(0)), 1.5::Decimal(38, 30), CAST(NULL, 'Nullable(Decimal(38, 30))'))) SETTINGS compile_expressions = 1;
SELECT 'nullable_picks_null_no_jit',  toString(if(materialize(toBool(0)), 1.5::Decimal(38, 30), CAST(NULL, 'Nullable(Decimal(38, 30))'))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: identity (`from_type == to_type`), short-circuit return of original
-- value. Triggered when both branches have IDENTICAL Decimal type (both
-- precision and scale match), so `nativeCastWithDecimalScale` returns the
-- input value unchanged.
-- ============================================================================
SELECT 'identity_dec18_4:jit',     toString(if(materialize(toBool(1)), 1.5::Decimal(18, 4), 2.5::Decimal(18, 4))) SETTINGS compile_expressions = 1;
SELECT 'identity_dec18_4:no_jit',  toString(if(materialize(toBool(1)), 1.5::Decimal(18, 4), 2.5::Decimal(18, 4))) SETTINGS compile_expressions = 0;

SELECT 'identity_dec76_60:jit',     toString(if(materialize(toBool(0)), 1.5::Decimal(76, 60), 2.5::Decimal(76, 60))) SETTINGS compile_expressions = 1;
SELECT 'identity_dec76_60:no_jit',  toString(if(materialize(toBool(0)), 1.5::Decimal(76, 60), 2.5::Decimal(76, 60))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: `multiIf` with mixed integer literals + Decimal else
-- (the original failure mode from #103808). The integer arms exercise
-- `nativeCastWithDecimalScale` Int -> Decimal at the result type.
-- ============================================================================
SELECT 'multiif_dec38_30_pick0:jit',     toString(multiIf(materialize(toBool(1)), 1, toBool(0), 2, 3.5::Decimal(38, 30))) SETTINGS compile_expressions = 1;
SELECT 'multiif_dec38_30_pick0:no_jit',  toString(multiIf(materialize(toBool(1)), 1, toBool(0), 2, 3.5::Decimal(38, 30))) SETTINGS compile_expressions = 0;

SELECT 'multiif_dec38_30_pickelse:jit',     toString(multiIf(materialize(toBool(0)), 1, toBool(0), 2, 3.5::Decimal(38, 30))) SETTINGS compile_expressions = 1;
SELECT 'multiif_dec38_30_pickelse:no_jit',  toString(multiIf(materialize(toBool(0)), 1, toBool(0), 2, 3.5::Decimal(38, 30))) SETTINGS compile_expressions = 0;

SELECT 'multiif_dec76_60:jit',     toString(multiIf(materialize(toBool(1)), 1, toBool(0), 2, 3.5::Decimal(76, 60))) SETTINGS compile_expressions = 1;
SELECT 'multiif_dec76_60:no_jit',  toString(multiIf(materialize(toBool(1)), 1, toBool(0), 2, 3.5::Decimal(76, 60))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Materialized columns from a subquery (the original failure from #103808).
-- ============================================================================
SELECT 'materialized_dec18_7:jit',
    toString(sum(if(k != 1, r, 1)))
FROM (SELECT materialize(1::Decimal(18, 7)) AS r, materialize(1) AS k)
SETTINGS compile_expressions = 1;
SELECT 'materialized_dec18_7:no_jit',
    toString(sum(if(k != 1, r, 1)))
FROM (SELECT materialize(1::Decimal(18, 7)) AS r, materialize(1) AS k)
SETTINGS compile_expressions = 0;

SELECT 'materialized_dec38_30:jit',
    toString(sum(if(k != 1, r, 1)))
FROM (SELECT materialize(1::Decimal(38, 30)) AS r, materialize(1) AS k)
SETTINGS compile_expressions = 1;
SELECT 'materialized_dec38_30:no_jit',
    toString(sum(if(k != 1, r, 1)))
FROM (SELECT materialize(1::Decimal(38, 30)) AS r, materialize(1) AS k)
SETTINGS compile_expressions = 0;

SELECT 'materialized_dec76_73:jit',
    toString(sum(if(k != 1, r, 1)))
FROM (SELECT materialize(1::Decimal(76, 73)) AS r, materialize(1) AS k)
SETTINGS compile_expressions = 1;
SELECT 'materialized_dec76_73:no_jit',
    toString(sum(if(k != 1, r, 1)))
FROM (SELECT materialize(1::Decimal(76, 73)) AS r, materialize(1) AS k)
SETTINGS compile_expressions = 0;

-- Materialized integer + materialized Decimal branch
-- (exercises Int -> Decimal helper with non-constant integer input).
SELECT 'materialized_int_to_dec38_30:jit',
    toString(sum(if(k != 0, r, k)))
FROM (SELECT materialize(1.5::Decimal(38, 30)) AS r, materialize(toInt64(7)) AS k)
SETTINGS compile_expressions = 1;
SELECT 'materialized_int_to_dec38_30:no_jit',
    toString(sum(if(k != 0, r, k)))
FROM (SELECT materialize(1.5::Decimal(38, 30)) AS r, materialize(toInt64(7)) AS k)
SETTINGS compile_expressions = 0;

-- ============================================================================
-- multiIf over a multi-row table with mixed branch types.
-- Each row picks a different branch, exercising the JIT path through
-- materialized integer keys with a high-scale Decimal else branch.
-- ============================================================================
DROP TABLE IF EXISTS jit_decimal_parity_input;
CREATE TABLE jit_decimal_parity_input (idx UInt32, k Int64) ENGINE = MergeTree ORDER BY idx;
INSERT INTO jit_decimal_parity_input VALUES (0, 0), (1, 1), (2, 2), (3, -7), (4, 100);

SELECT 'multirow_multiif:jit',
    arraySort(groupArray(toString(multiIf(k = 0, 1, k = 1, 2, k = 2, 3, 5.5::Decimal(38, 30)))))
FROM jit_decimal_parity_input
SETTINGS compile_expressions = 1;
SELECT 'multirow_multiif:no_jit',
    arraySort(groupArray(toString(multiIf(k = 0, 1, k = 1, 2, k = 2, 3, 5.5::Decimal(38, 30)))))
FROM jit_decimal_parity_input
SETTINGS compile_expressions = 0;

DROP TABLE jit_decimal_parity_input;
