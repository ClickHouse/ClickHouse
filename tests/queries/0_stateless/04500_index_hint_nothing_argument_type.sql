-- indexHint ignores its arguments and always returns UInt8. A Nothing-typed
-- argument must not force the declared return type to Nothing (which mismatched
-- the UInt8 column produced at execution and propagated up, tripping a chassert
-- inside anyLast_respect_nulls). See indexHint.h useDefaultImplementationForNothing.

SELECT toTypeName(indexHint(assumeNotNull(materialize(NULL))));
SELECT indexHint(assumeNotNull(materialize(NULL)));

-- The original fuzzer-found chain: a Nothing arg flows through indexHint into an
-- aggregate state combinator. Must not abort (LOGICAL_ERROR 'returns_nullable_type').
SELECT anyLastRespectNullsStateOrDefaultDistinct(divide(toFixedString(NULL, JSONExtractBoolCaseInsensitive(indexHint(assumeNotNull(toString(materialize(NULL), 1025))), toString(NULL))), ';--')) GROUP BY ALL FORMAT Null;

-- indexHint keeps working normally.
SELECT toTypeName(indexHint(1)), indexHint(1), indexHint(NULL);

-- The sibling ignore() has the same declared-result invariant (always UInt8), so a
-- Nothing-typed argument must not rewrite its declared return type to Nothing either.
-- See ignore.cpp useDefaultImplementationForNothing. Only the declared type is checked:
-- unlike indexHint, ignore evaluates its arguments, and a non-empty Nothing column
-- cannot be materialized, so the value form is not evaluable and is not the bug here.
SELECT toTypeName(ignore(assumeNotNull(materialize(NULL))));
SELECT toTypeName(ignore(1)), ignore(1), ignore(NULL);

-- isZeroOrNull explicitly accepts a Nothing argument (dedicated Nothing branch in
-- getReturnTypeImpl/executeImpl always yields UInt8), so the default Nothing-dispatch
-- must not rewrite its declared return type to Nothing either. See isZeroOrNull.cpp
-- useDefaultImplementationForNothing. As with ignore, only the declared type is checked:
-- the argument assumeNotNull(materialize(NULL)) cannot be materialized as a non-empty
-- Nothing column, so the value form is not evaluable and is not the bug here.
SELECT toTypeName(isZeroOrNull(assumeNotNull(materialize(NULL))));
SELECT toTypeName(isZeroOrNull(1)), isZeroOrNull(1), isZeroOrNull(0), isZeroOrNull(NULL);

-- The random and id generators take an optional trailing argument that is ignored and
-- exists only to suppress common subexpression elimination. A Nothing-typed argument
-- there must not rewrite the declared return type either. As with ignore, only the
-- declared type is checked: the argument cannot be materialized as a non-empty Nothing
-- column, so the value form is not evaluable and is not the bug here.
SELECT toTypeName(randConstant(assumeNotNull(materialize(NULL)))), toTypeName(rand(assumeNotNull(materialize(NULL)))), toTypeName(rand64(assumeNotNull(materialize(NULL)))), toTypeName(randCanonical(assumeNotNull(materialize(NULL))));
SELECT toTypeName(randBernoulli(0.5, assumeNotNull(materialize(NULL)))), toTypeName(randNormal(0, 1, assumeNotNull(materialize(NULL)))), toTypeName(randPoisson(1, assumeNotNull(materialize(NULL))));
SELECT toTypeName(randomString(5, assumeNotNull(materialize(NULL)))), toTypeName(randomPrintableASCII(5, assumeNotNull(materialize(NULL))));
SELECT toTypeName(generateUUIDv4(assumeNotNull(materialize(NULL)))), toTypeName(generateUUIDv7(assumeNotNull(materialize(NULL)))), toTypeName(generateULID(assumeNotNull(materialize(NULL)))), toTypeName(generateSnowflakeID(assumeNotNull(materialize(NULL))));

-- They keep working normally, and LowCardinality is still propagated.
SELECT randConstant(1) >= 0, rand(1) >= 0, randNormal(0, 1, 1) IS NOT NULL, length(randomString(5, 1)), toTypeName(randConstant(toLowCardinality('a')));

-- Arguments that are not the ignored trailing one are still validated.
SELECT randomString('notanumber'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT randBernoulli('notanumber'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- More than one argument is still rejected, whether or not one of them is Nothing.
-- The GROUP BY key position is required: it is where a malformed function reaches the
-- query tree validator.
SELECT 1 GROUP BY randConstant(assumeNotNull(materialize(NULL)), 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT 1 GROUP BY randConstant(1, 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
