-- A lambda passed where a higher-order function expects a concrete value (e.g. the
-- arrayFold accumulator) used to segfault: getLambdaArgumentTypes left the placeholder
-- DataTypeFunction's argument types null, and the null DataTypePtr was later dereferenced
-- (FunctionArrayMapped::getReturnTypeImpl). Lambda arguments must be validated up front,
-- before any lambda body is visited/resolved, otherwise a later unresolved lambda copied
-- into an earlier lambda's argument type sends the earlier body down the non-lambda path.
-- Reject cleanly under both analyzers instead.

SET enable_analyzer = 1;
SELECT arrayFold((acc, x) -> x, [1, 2, 3], ((acc, x) -> x)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayFold((acc, x) -> arrayFilter(acc, [(x, x)]), [1], ((a, b) -> b)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Valid arrayFold still works under both analyzers.
SET enable_analyzer = 1;
SELECT arrayFold((acc, x) -> acc + x, [1, 2, 3, 4], toInt64(0));
