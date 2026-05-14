-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103817
-- UndefinedBehaviorSanitizer in FieldVisitorConvertToNumber.h:85:22
--
-- The bug: for wide integer types (UInt64, Int64, Int128, UInt128, Int256, UInt256),
-- `Float64(numeric_limits<T>::max())` rounds UP because the integer max has more than
-- 53 significant bits. The check `x > Float64(numeric_limits<T>::max())` therefore
-- fails to reject `Float64` values equal to that rounded-up boundary, producing UB
-- in the subsequent `static_cast<T>(x)`.
--
-- Reproducer: `Float64(2^64)` (= 1.8446744073709552e+19) is exactly representable in
-- `Float64` but is out of range for `UInt64`. The fix uses `accurate::greaterOp` /
-- `DecomposedFloat::greater` which correctly rejects the boundary value.

-- Aggregate function path: parameter routed through `FieldVisitorConvertToNumber<UInt64>`.
-- The boundary value `1.8446744073709552e19` (= 2^64) must be rejected, not silently cast.
SELECT topK(1.8446744073709552e19)(number) FROM numbers(0); -- { serverError CANNOT_CONVERT_TYPE }
SELECT topK(-1.5)(number) FROM numbers(0); -- { serverError CANNOT_CONVERT_TYPE }
SELECT uniqUpTo(1.8446744073709552e19)(number) FROM numbers(0); -- { serverError CANNOT_CONVERT_TYPE }

-- In-range Float should still work (parameter truncates to integer).
SELECT length(topK(3.5)(number)) FROM numbers(10);

-- Settings path: `SettingFieldNumber<T>::operator=` -> `fieldToNumber<T>` (`SettingsFields.cpp:119`).
-- Same UB site, same fix. `SET` (server-side) is used so the test runner sees a server error.
SET max_threads = 1.8446744073709552e19; -- { serverError CANNOT_CONVERT_TYPE }
SET max_threads = -1.5; -- { serverError CANNOT_CONVERT_TYPE }

-- In-range Float for an integer setting truncates to integer (no error).
SET max_block_size = 4096.5;
SELECT getSetting('max_block_size');
