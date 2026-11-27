-- arrayPartialSort / arrayPartialReverseSort must honor a per-row (non-constant) limit argument,
-- not just the limit of the first row.

SELECT number + 1 AS k, arrayResize(arrayPartialSort(k, materialize([5, 4, 3, 2, 1])), k) FROM numbers(5) ORDER BY k;

SELECT number + 1 AS k, arrayResize(arrayPartialReverseSort(k, materialize([1, 2, 3, 4, 5])), k) FROM numbers(5) ORDER BY k;

SELECT id, arrayResize(arrayPartialSort(k, arr), k)
FROM values('id UInt8, k UInt8, arr Array(Int32)', (1, 1, [3, 1, 2]), (2, 3, [9, 7, 8, 6]), (3, 2, [5, 5, 4, 4, 3]))
ORDER BY id;

-- The same with a lambda, which exercises a different argument layout.
SELECT number + 1 AS k, arrayResize(arrayPartialSort((x) -> -x, k, materialize([1, 2, 3, 4, 5])), k) FROM numbers(5) ORDER BY k;

-- A constant limit must keep working as before.
SELECT arrayResize(arrayPartialSort(2, [5, 9, 1, 3]), 2);
SELECT arrayResize(arrayPartialReverseSort(2, [5, 9, 1, 3]), 2);
