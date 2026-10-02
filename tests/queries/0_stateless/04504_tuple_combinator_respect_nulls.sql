-- The respect-nulls variants (`anyRespectNulls` and friends) are marked as window functions, so the
-- factory does not apply the automatic `-Null` combinator that otherwise strips `Nullable` from the
-- argument types before a combinator sees them. `-Tuple` therefore receives the experimental
-- `Nullable(Tuple)` type as-is and rejects it at resolution time: lifting a whole-tuple NULL into
-- per-element NULLs is not implemented. Per-element respect-nulls semantics are available by making
-- the elements `Nullable` instead of the tuple itself.

SET allow_experimental_nullable_tuple_type = 1;

SELECT 'ordinary aggregate over Nullable(Tuple) goes through the -Null wrapper';
SELECT anyTuple(if(number = 0, NULL, CAST((number, number * 2), 'Nullable(Tuple(UInt64, UInt64))'))) FROM numbers(3);

SELECT 'respect-nulls over a plain Tuple';
SELECT anyRespectNullsTuple((number, number * 2)) FROM numbers(3);

SELECT 'respect-nulls over Nullable elements';
SELECT anyRespectNullsTuple(CAST(if(number = 0, (NULL, NULL), (number, number * 2)), 'Tuple(Nullable(UInt64), Nullable(UInt64))')) AS r, toTypeName(r) FROM numbers(3);

SELECT 'errors';
SELECT anyRespectNullsTuple(if(number = 0, NULL, CAST((number, number * 2), 'Nullable(Tuple(UInt64, UInt64))'))) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT anyTuple(if(number = 0, NULL, CAST((number, number * 2), 'Nullable(Tuple(UInt64, UInt64))'))) RESPECT NULLS FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
