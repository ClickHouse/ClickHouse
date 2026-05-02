-- Test: const/non-const fast path size-mismatch check in arrayDotProduct.
-- Covers: src/Functions/array/arrayDotProduct.cpp:346 — the [[unlikely]] branch
--   `if (offsets_x[0] != offset_y - prev_offset)` inside `executeWithLeftArgConst`.
-- The existing test in 02708_dotProduct.sql uses two const arguments which (because
-- `useDefaultImplementationForConstants() = true`) takes the non-const/non-const path
-- and triggers the `hasEqualOffsets` check at line 268, NOT this code path.
-- This path was added/modified by the correctness fix in PR #60928 (one-const +
-- one-non-const had a wrong offset comparison before the fix).

DROP TABLE IF EXISTS tab_dp_size_check;
CREATE TABLE tab_dp_size_check (id UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_dp_size_check VALUES (0, [1.0, 2.0, 3.0, 4.0]);

-- left arg const, right arg non-const, mismatched sizes -> hits the lines 343-356 size check
SELECT id, dotProduct([1.0::Float32, 2.0::Float32], vec) FROM tab_dp_size_check; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

-- right arg const, left arg non-const, mismatched sizes -> swap branch, then same size check
SELECT id, dotProduct(vec, [1.0::Float32, 2.0::Float32]) FROM tab_dp_size_check; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

-- arrayDotProduct alias version, left const, mismatched sizes
SELECT id, arrayDotProduct([1.0::Float32, 2.0::Float32, 3.0::Float32], vec) FROM tab_dp_size_check; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

-- Confirm the matching-size case still works in this same const/non-const code path.
SELECT id, dotProduct([1.0::Float32, 2.0::Float32, 3.0::Float32, 4.0::Float32], vec) FROM tab_dp_size_check ORDER BY id;
SELECT id, dotProduct(vec, [1.0::Float32, 2.0::Float32, 3.0::Float32, 4.0::Float32]) FROM tab_dp_size_check ORDER BY id;

DROP TABLE tab_dp_size_check;
