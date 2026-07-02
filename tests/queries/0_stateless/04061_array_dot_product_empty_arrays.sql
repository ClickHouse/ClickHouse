-- Regression test: arrayDotProduct must handle empty arrays without UB.
-- Previously, &data[offset] with offset == size() was undefined behavior.

-- Empty arrays of various types
SELECT arrayDotProduct([]::Array(Float32), []::Array(Float32));
SELECT arrayDotProduct([]::Array(Float64), []::Array(Float64));
SELECT arrayDotProduct([]::Array(UInt8), []::Array(UInt8));

-- Mixed empty/non-empty via table (exercises per-row offset logic)
SELECT arrayDotProduct(x, y) FROM VALUES('x Array(Float32), y Array(Float32)',
    ([], []),
    ([1, 2, 3], [4, 5, 6]),
    ([], []),
    ([10], [20]));

-- Const-left empty array (exercises executeWithLeftArgConst path)
SELECT arrayDotProduct([]::Array(Float32), y) FROM VALUES('y Array(Float32)', ([]), ([]));
SELECT arrayDotProduct([]::Array(Float64), y) FROM VALUES('y Array(Float64)', ([]), ([]));

-- Array size exactly divisible by VEC_SIZE (4): ensures the last aligned chunk
-- is processed by the unrolled loop, not the scalar tail.
-- Uses Int32 to exercise the scalar fallback path (ResultType = Int64).
SELECT arrayDotProduct([1, 2, 3, 4]::Array(Int32), [5, 6, 7, 8]::Array(Int32));
SELECT arrayDotProduct([1, 2, 3, 4, 5, 6, 7, 8]::Array(Int32), [1, 1, 1, 1, 1, 1, 1, 1]::Array(Int32));
