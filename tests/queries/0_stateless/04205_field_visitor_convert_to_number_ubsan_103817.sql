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

-- Coverage for FieldVisitorConvertToNumber.h: operator() overloads for non-numeric Field
-- types (Null, String, Tuple, Map) and edge cases (non-finite Float64, Decimal parameter).
-- These paths had zero CI coverage because no test passed such a typed literal to
-- an aggregate function constructor before.

-- 1. Null parameter → CANNOT_CONVERT_TYPE
SELECT topK(NULL)(number) FROM numbers(3); -- { serverError CANNOT_CONVERT_TYPE }

-- 2. String parameter → CANNOT_CONVERT_TYPE
SELECT topK('abc')(number) FROM numbers(3); -- { serverError CANNOT_CONVERT_TYPE }

-- 3. Tuple parameter → CANNOT_CONVERT_TYPE
SELECT topK((1, 2))(number) FROM numbers(3); -- { serverError CANNOT_CONVERT_TYPE }

-- 4. Map parameter → CANNOT_CONVERT_TYPE
SELECT topK(map('a', 1))(number) FROM numbers(3); -- { serverError CANNOT_CONVERT_TYPE }

-- 5. Non-finite Float64 to integer type → CANNOT_CONVERT_TYPE
SELECT topK(inf)(number) FROM numbers(3);  -- { serverError CANNOT_CONVERT_TYPE }
SELECT topK(nan)(number) FROM numbers(3);  -- { serverError CANNOT_CONVERT_TYPE }

-- 6. Decimal32 parameter to integer: (x.getValue() / x.getScaleMultiplier()).convertTo<T>()
SELECT length(topK(3::Decimal32(0))(number)) = 3 AS ok FROM numbers(10);
