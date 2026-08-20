-- Tags: no-fasttest
-- Regression for issue #105649: planner-time LOGICAL_ERROR
-- 'Unexpected return type from if. Expected Variant(Int64, UInt64). Got Nullable(Int64)'.
--
-- The `IfConstantConditionPass` folds `if(1, then, else) -> then`. The picked
-- branch and the original `if` have types that compare equal under
-- `DataTypeNullable::equals` / `DataTypeNumber::equals`, but differ in the
-- `canUnsignedBeSigned` flag carried on `DataTypeUInt64`. After the fold, the
-- outer `if`'s arguments expose the `canSigned` flag that the analyzer had
-- erased on its own intermediate type. `FunctionIf::executeGeneric` re-derived
-- the common type from the live argument types via
-- `getLeastSupertypeOrVariant`, hit `convertUInt64toInt64IfPossible`, and
-- produced `Nullable(Int64)` instead of the declared `Variant(Int64, UInt64)`,
-- triggering the assertion in `executeActionForPartialResult` at planning time.

SET enable_analyzer = 1;
SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

-- Minimal reproducer: no table required.
SELECT toTypeName(
    if(materialize(1::UInt8),
       1::Nullable(Int64),
       if(1, if(materialize(1::UInt8), NULL, 9223372036854775807), 50::UInt32))
);

SELECT
    if(materialize(1::UInt8),
       1::Nullable(Int64),
       if(1, if(materialize(1::UInt8), NULL, 9223372036854775807), 50::UInt32));

-- Variation: cond false picks the inner-if branch.
SELECT
    if(materialize(0::UInt8),
       1::Nullable(Int64),
       if(1, if(materialize(1::UInt8), NULL, 9223372036854775807), 50::UInt32));

-- Variation: cond is Nullable.
SELECT
    if(materialize(toNullable(1::UInt8)),
       1::Nullable(Int64),
       if(1, if(materialize(1::UInt8), NULL, 9223372036854775807), 50::UInt32));

-- The original AST-fuzzer-found shape from the issue, slimmed down.
DROP TABLE IF EXISTS t_issue_105649;
CREATE TABLE t_issue_105649 (a UInt32, b Nullable(Int64)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_issue_105649 SELECT number, if(number%3=0, NULL, number*10) FROM numbers(20);

SELECT (
    SELECT
        if(10 <= t.b, t.b,
           if(1,
              if(100 >= t.a, NULL, 9223372036854775807),
              t.a))
) AS x
FROM t_issue_105649 AS t
ORDER BY t.a
LIMIT 5
SETTINGS allow_experimental_correlated_subqueries = 1;

DROP TABLE t_issue_105649;
