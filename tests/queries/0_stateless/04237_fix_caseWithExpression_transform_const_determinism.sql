-- Regression test for the `transform` default-branch conversion bug that also surfaced as
-- non-determinism in `caseWithExpression` (different result when arguments are made `const`),
-- caught by `FunctionsStress.stress` in `Unit tests (tsan, function_prop_fuzzer)`.
--
-- Root cause: `transform`'s `initializeTransformCache` built `cache->default_column` via
-- `convertFieldToType((*default_col)[0], *result_type) + default_column->insert(f)`. The `Field`
-- abstraction collapses some types: a `FixedString` value becomes a `String` `Field` keeping the
-- raw bytes (no trailing-zero trimming), and an `Enum` value becomes its underlying integer
-- `Field`. So when the cached default went through the `String` representation, `FixedString`
-- defaults kept their trailing `\0` bytes and `Enum` defaults turned into their numeric value --
-- both disagree with `castColumn(default, result_type)` (which is what the matched-branch
-- `cache->to_column` already used).
--
-- `caseWithExpression` exposes this divergence because it has two execution paths: a fast
-- `transform`-based path when all `WHEN`/`THEN` values are constant, and a `multiIf`-based path
-- otherwise. The `FunctionsStress.stress` "broken_determinism" check re-runs each call with
-- random arguments turned `const`, which can flip between the two paths and reveal the bug.

-- Direct `transform` checks: the default-branch result must agree with `cast(default AS result_type)`.

-- FixedString default, String result_type -- trailing zeros must be trimmed.
SELECT length(transform(toUInt32(0), [toUInt32(1)], ['a'], toFixedString('b', 3))) = length(cast(toFixedString('b', 3) AS String));
SELECT transform(toUInt32(0), [toUInt32(1)], ['a'], toFixedString('b', 3)) = cast(toFixedString('b', 3) AS String);

-- Enum default, String result_type -- value's name must be used, not its numeric storage.
SELECT transform(toUInt32(0), [toUInt32(1)], ['a'], cast('cV1' AS Enum8('cV1' = 42))) = cast(cast('cV1' AS Enum8('cV1' = 42)) AS String);
SELECT transform(toUInt32(0), [toUInt32(1)], ['a'], cast('cV2' AS Enum16('cV2' = -15009))) = cast(cast('cV2' AS Enum16('cV2' = -15009)) AS String);

-- `caseWithExpression`: const vs non-const arguments must produce the same result. The first
-- variant of each pair enters the `transform` path (all `WHEN`/`THEN` const); the second variant
-- enters the `multiIf` path (a `WHEN` is `materialize`d, so not `const`).

-- FixedString `ELSE`.
SELECT
    caseWithExpression(materialize(toUInt32(99)), toUInt64(1), 'match', toFixedString('a', 3))
  = caseWithExpression(materialize(toUInt32(99)), materialize(toUInt64(1)), 'match', toFixedString('a', 3));

-- Enum8 `ELSE`.
SELECT
    caseWithExpression(toUInt32(0), toUInt64(1), toFixedString('foo', 3), cast('cV1' AS Enum8('cV1' = 42)))
  = caseWithExpression(toUInt32(0), materialize(toUInt64(1)), toFixedString('foo', 3), cast('cV1' AS Enum8('cV1' = 42)));

-- Enum16 `ELSE`.
SELECT
    caseWithExpression(toUInt32(0), toUInt64(1), 'foo', cast('cV2' AS Enum16('cV2' = -15009)))
  = caseWithExpression(toUInt32(0), materialize(toUInt64(1)), 'foo', cast('cV2' AS Enum16('cV2' = -15009)));

-- `caseWithExpression` over many rows -- both paths must produce identical columns.
SELECT countIf(t <> m) = 0
FROM
(
    SELECT
        caseWithExpression(toUInt32(number % 3), toUInt64(1), 'match', toFixedString('a', 3)) AS t,
        caseWithExpression(toUInt32(number % 3), materialize(toUInt64(1)), 'match', toFixedString('a', 3)) AS m
    FROM numbers(20)
);

SELECT countIf(t <> m) = 0
FROM
(
    SELECT
        caseWithExpression(toUInt32(number % 3), toUInt64(1), 'foo', cast('cV1' AS Enum8('cV1' = 42))) AS t,
        caseWithExpression(toUInt32(number % 3), materialize(toUInt64(1)), 'foo', cast('cV1' AS Enum8('cV1' = 42))) AS m
    FROM numbers(20)
);

-- Direct reproducers reduced from the failing `FunctionsStress.stress` queries.

-- Original failure: 'cV12' (multiIf) vs '48' (transform).
SELECT
    caseWithExpression(
        cast(0 AS Nullable(UInt16)),
        toUInt64(4511644536118997743),
        toFixedString('placeholder', 11),
        cast('cV12' AS Enum8(
            'cV1' = -113, 'cV12' = 48, 'cV3' = 77)))
  = caseWithExpression(
        cast(0 AS Nullable(UInt16)),
        materialize(toUInt64(4511644536118997743)),
        toFixedString('placeholder', 11),
        cast('cV12' AS Enum8(
            'cV1' = -113, 'cV12' = 48, 'cV3' = 77)));

-- Original failure: '255.207.171.58' (multiIf) vs '255.207.171.58\0\0\0...' (transform).
SELECT
    caseWithExpression(
        toUInt32(1319184),
        toUInt128(3726663760157237046072062916965),
        cast('cV3' AS Enum16('cV3' = -18651, 'cV4' = 17986)),
        toInt16(29771),
        cast('-9202355639269' AS LowCardinality(Nullable(String))),
        toFixedString('255.207.171.58', 61))
  = caseWithExpression(
        toUInt32(1319184),
        materialize(toUInt128(3726663760157237046072062916965)),
        cast('cV3' AS Enum16('cV3' = -18651, 'cV4' = 17986)),
        materialize(toInt16(29771)),
        materialize(cast('-9202355639269' AS LowCardinality(Nullable(String)))),
        materialize(toFixedString('255.207.171.58', 61)));
