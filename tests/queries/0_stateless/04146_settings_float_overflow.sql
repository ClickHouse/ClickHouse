-- Test: exercises `fieldToNumber` Float64 out-of-range branch in `SettingsFields.cpp`
-- Covers: src/Core/SettingsFields.cpp — `if (x > Float64(numeric_limits<T>::max()) || x < Float64(numeric_limits<T>::lowest()))`
-- when a setting receives a finite Float64 value that is outside the integer type's range.
-- The PR adds the branch but only tests `nan` (non-finite path) and `-1` (Int64 path).
-- This test exercises both the upper-bound (1e30) and lower-bound (-1.5) sides of the Float64 range check.

-- Float64 above UInt64 range -> CANNOT_CONVERT_TYPE (out of range branch)
SET max_threads = 1e30; -- { serverError CANNOT_CONVERT_TYPE }

-- Negative Float64 (finite) for unsigned setting -> CANNOT_CONVERT_TYPE (out of range branch)
SET max_block_size = -1.5; -- { serverError CANNOT_CONVERT_TYPE }

-- In-range Float64 fits -> truncates to integer (no error)
SET max_block_size = 1.5;
SELECT getSetting('max_block_size');
