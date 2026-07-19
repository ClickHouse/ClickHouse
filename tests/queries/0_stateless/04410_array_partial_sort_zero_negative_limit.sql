-- arrayPartialSort / arrayPartialReverseSort with limit = 0 must return the source array unchanged
-- (no sorting), and a negative limit must raise an exception (like arrayTopK does for its K argument).

-- A constant limit of 0 returns the source array unchanged.
SELECT arrayPartialSort(0, [5, 9, 1, 3]);
SELECT arrayPartialReverseSort(0, [5, 9, 1, 3]);

-- The same with a lambda.
SELECT arrayPartialSort((x) -> -x, 0, [5, 9, 1, 3]);

-- The same with a non-constant (per-row) zero limit.
SELECT arrayPartialSort(materialize(0), materialize([5, 9, 1, 3]));

-- Per-row limit: a limit of 0 leaves the row unchanged, a positive limit still sorts.
SELECT id, arrayPartialSort(k, arr)
FROM values('id UInt8, k UInt8, arr Array(Int32)', (1, 0, [3, 1, 2]), (2, 4, [9, 7, 8, 6]))
ORDER BY id;

-- A negative constant limit raises an exception.
SELECT arrayPartialSort(-1, [5, 9, 1, 3]); -- { serverError BAD_ARGUMENTS }
SELECT arrayPartialReverseSort(-2, [5, 9, 1, 3]); -- { serverError BAD_ARGUMENTS }
SELECT arrayPartialSort((x) -> -x, -1, [5, 9, 1, 3]); -- { serverError BAD_ARGUMENTS }

-- A negative per-row limit raises an exception.
SELECT arrayPartialSort(k, arr)
FROM values('k Int8, arr Array(Int32)', (1, [3, 1, 2]), (-1, [9, 7, 8])); -- { serverError BAD_ARGUMENTS }

-- A wide integer limit type is accepted; the value is read at its full width. Only the first `limit`
-- elements are guaranteed sorted, so slice to keep the deterministic prefix.
SELECT arraySlice(arrayPartialSort(CAST(2 AS Int128), [5, 9, 1, 3]), 1, 2);
SELECT arraySlice(arrayPartialReverseSort(CAST(2 AS UInt128), [5, 9, 1, 3]), 1, 2);

-- A huge wide value means "sort everything": it is saturated to the array size, not truncated.
SELECT arrayPartialSort(CAST('100000000000000000000' AS UInt128), [5, 9, 1, 3]);

-- A negative wide value still raises an exception (a value error, like a negative native value).
SELECT arrayPartialSort(CAST(-1 AS Int128), [5, 9, 1, 3]); -- { serverError BAD_ARGUMENTS }
SELECT arrayPartialReverseSort(CAST(-2 AS Int256), [5, 9, 1, 3]); -- { serverError BAD_ARGUMENTS }
