-- Tags: no-fasttest
-- Tests argument-validation error branches in h3ToCenterChild.cpp that had
-- zero CI coverage. Existing tests (02155, 02223, 04034) only pass valid UInt64
-- cell indexes with valid UInt8 resolutions; none trigger the type checks or
-- the runtime resolution bounds check.
--
-- COVERAGE TARGETS  (src/Functions/h3ToCenterChild.cpp)
--
--   Lines 46-50: getReturnTypeImpl checks that argument 1 is UInt64.
--     h3ToCenterChild stores cell indexes as UInt64 (the H3 library's native
--     type). Passing any other type (String, Int32, etc.) triggers
--     ILLEGAL_TYPE_OF_ARGUMENT at analysis time. Without the check, a
--     mismatched pointer cast would produce garbage H3 library results.
--
--   Lines 53-57: getReturnTypeImpl checks that argument 2 is UInt8.
--     The resolution parameter must be UInt8. Passing UInt64 (a common mistake
--     since numeric literals default to a wider type) triggers
--     ILLEGAL_TYPE_OF_ARGUMENT. Without the check, a truncated or sign-extended
--     value would be passed to the H3 library.
--
--   Lines 99-105: executeImpl checks the runtime resolution value against
--     MAX_H3_RES (15). Even with a valid UInt8 type, a value of 16 or greater
--     is outside the H3 specification. Without this guard, the H3 library call
--     would be made with an invalid resolution, returning undefined results.

-- Argument 1 must be UInt64; String is rejected (lines 46-50).
SELECT h3ToCenterChild('hello', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Argument 2 must be UInt8; UInt64 is rejected even though sizes differ (lines 53-57).
SELECT h3ToCenterChild(617733192::UInt64, 10::UInt64); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Resolution 16 exceeds MAX_H3_RES = 15 at runtime (lines 99-105).
SELECT h3ToCenterChild(617733192::UInt64, 16::UInt8); -- { serverError ARGUMENT_OUT_OF_BOUND }
