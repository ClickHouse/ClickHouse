-- Regression test for issue #72714: analyzer must respect short-circuit semantics
-- for constant `multiIf` / `if` expressions and avoid evaluating dead branches
-- during analysis-time constant folding.

SET enable_analyzer = 1;
SET allow_suspicious_low_cardinality_types = 1;

-- Canonical issue #72714 reproducer.
WITH 0 AS n SELECT multiIf(n = 0, 0, intDiv(100, n));

-- Canonical TOO_LARGE_STRING_SIZE branch shape, using constants.
WITH 99 AS atype, 'xxxxxxxxxxxxxxxxx' AS s, CAST('0.0.0.0', 'IPv4') AS IPv4
SELECT multiIf(atype = 1, IPv4,
               atype = 28, toFixedString(s, 16),
               s);

-- Nested short-circuit should also avoid dead branches.
WITH 1 AS flag SELECT if(flag = 1, 'ok', toFixedString('too_long_string_17_', 10));

-- LowCardinality / Nullable branch remains safe.
SELECT multiIf(
    CAST(0, 'LowCardinality(UInt8)') = 1,
    toFixedString('x', 1),
    'ok');

-- Fuzzer-shaped regression case from the reverted approach.
SELECT and(materialize(0), if(0, toFixedString('str_1', 5), [1, 2, 3]) IS NOT NULL);

-- Sanity: valid constant folding paths still work.
SELECT if(1, 2, 3);
SELECT multiIf(1, 'a', 0, 'b', 'c');

-- Regression: constant multiIf must preserve common-supertype semantics.
-- The early-fold path must fall through when dead branches resolve cleanly, so that
-- FunctionMultiIf::build runs normal type unification instead of replacing the node
-- with the (narrower) winning branch.
SELECT toTypeName(multiIf(1, toUInt8(1), toUInt16(2)));
SELECT toTypeName(multiIf(0, toUInt8(1), 1, toUInt16(2), toUInt32(3)));
SELECT toTypeName(multiIf(1, toNullable(toUInt8(1)), toUInt16(2)));
