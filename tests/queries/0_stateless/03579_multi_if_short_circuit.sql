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

-- Regression: AST fuzzer SEGV at resolveFunction.cpp (multiIf early-fold),
-- caused by indexing a vector slot that was rewritten during nested
-- resolveExpressionNode. The query must terminate analysis cleanly (any
-- analyzer-level error is acceptable; the test only guards against crashing).
SELECT NULL AS a
GROUP BY
    if(multiIf(not(lessOrEquals(moduloOrZero(*, 256), lessOrEquals((SELECT multiply(NULL, materialize(toNullable(1048576)))), 1048577))),
    divide(isNullable(256), intDiv(2147483646, toInt128(toInt32(divide(-2,
        multiIf(greater(PrettyMonoBlock, modulo(65535, modulo(65537, divide(divide(1025, NULL), -1)))),
            concatAssumeInjective(NULL, multiply(toNullable(-2), toInt256(3)), assumeNotNull(1025)),
            equals(Pretty, concat(divide(divide(modulo(9223372036854775806, -2147483649), 65537), 65537), (SELECT NULL), (SELECT (SELECT 3 LIMIT 284)))),
            modulo(2147483648, divide(intDiv(-2147483649, 1025), 1023)),
            or(minus(*, -2), isNotNull(NULL)))), materialize(7)))), NULL),
    toFixedString('\0', 1048576)),
    toDecimal128(2., 17),
    materialize(toUInt64OrNull(toLowCardinality(1))))
QUALIFY greaterOrEquals(*, -9223372036854775807)
LIMIT -1
FORMAT Null; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT, CANNOT_PARSE_NUMBER, TYPE_MISMATCH, NUMBER_OF_ARGUMENTS_DOESNT_MATCH, BAD_ARGUMENTS, ILLEGAL_COLUMN, TOO_LARGE_STRING_SIZE, NO_COMMON_TYPE, UNKNOWN_IDENTIFIER, ILLEGAL_AGGREGATION }
