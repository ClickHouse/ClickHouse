-- Verify that `sumMap` and `sumMapWithOverflow` reject `Bool` values after
-- `DataTypeNumber::isSummable` was changed to return false for custom-named
-- types (PR #98976). The `Bool` type is a custom-named `UInt8` and must not
-- be treated as summable by aggregate functions.

-- sumMap with a Bool value array must throw ILLEGAL_TYPE_OF_ARGUMENT
SELECT sumMap([1, 2], [true, false]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- sumMapWithOverflow also rejects Bool value arrays
SELECT sumMapWithOverflow([1, 2], [true, false]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
