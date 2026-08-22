SELECT arrayDifference([toDecimal32(100.0000991821289, 0), -2147483647]) AS x; --{serverError DECIMAL_OVERFLOW}
