-- Regression test for the `flattenTuple` empty-result LOGICAL_ERROR.
--
-- Previously, calling `flattenTuple` with an argument whose nested structure
-- contains only empty `Tuple()` leaves (directly or wrapped in arrays/tuples)
-- recursed to zero scalar columns. The resulting `ColumnTuple::create({})`
-- call hit the `LOGICAL_ERROR` guard that rejects empty tuple construction,
-- aborting the server in debug builds.
--
-- The fix detects the empty-flatten case in `getReturnTypeImpl` and throws
-- `ILLEGAL_TYPE_OF_ARGUMENT` with a user-readable message instead.

-- Direct empty Tuple() is already rejected by the existing check.
SELECT flattenTuple(CAST((tuple()) AS Tuple())); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Single-member tuple containing Array(Tuple()) — the original fuzzer-reduced case.
SELECT flattenTuple(CAST((tuple([])) AS Tuple(c0 Array(Tuple())))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Single-member tuple containing nested empty Tuple().
SELECT flattenTuple(CAST((tuple(tuple())) AS Tuple(c0 Tuple()))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Deeper nesting of empty tuples.
SELECT flattenTuple(CAST((tuple(tuple(tuple()))) AS Tuple(c0 Tuple(c00 Tuple())))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Double-array of empty tuple.
SELECT flattenTuple(CAST((tuple([[tuple()]])) AS Tuple(c0 Array(Array(Tuple()))))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The Nullable wrappers around the above empty-result shapes also reach the
-- same code path and must throw the same user-facing error.
SET allow_experimental_nullable_tuple_type = 1;
SELECT flattenTuple(CAST((tuple([])) AS Nullable(Tuple(c0 Array(Tuple()))))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT flattenTuple(CAST(NULL AS Nullable(Tuple(c0 Array(Tuple()))))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Sanity check: normal usage is unaffected.
SELECT flattenTuple(CAST((tuple(1)) AS Tuple(c0 Int32)));
SELECT flattenTuple(CAST((3, ('c', 4)) AS Tuple(a UInt32, b Tuple(c String, d UInt32))));
-- A tuple that mixes scalar members with arrays of empty tuples still has
-- something to flatten, so the call succeeds and the empty-tuple branches
-- contribute nothing to the output.
SELECT flattenTuple(CAST((tuple(1, [tuple()])) AS Tuple(c0 Int32, c1 Array(Tuple()))));
