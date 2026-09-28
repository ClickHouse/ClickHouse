-- Parameters of a binary encoded AggregateFunction type are Fields, and a Dynamic value carries its own type,
-- so the nesting depth of the parameters comes from the data. Deep nesting must be reported instead of
-- overflowing the stack. The payload below is SimpleAggregateFunction('x', [<Array of size 1> ...]).

SELECT * FROM format(RowBinary, 'd Dynamic', unhex(concat('2e017801', repeat('0d01', 300000)))) SETTINGS input_format_binary_max_type_complexity = 1000; -- { serverError INCORRECT_DATA }
SELECT * FROM format(RowBinary, 'd Dynamic', unhex(concat('2e017801', repeat('0f0100', 300000)))) SETTINGS input_format_binary_max_type_complexity = 1000; -- { serverError INCORRECT_DATA }

-- Without a complexity budget only the stack guard is left.
SELECT * FROM format(RowBinary, 'd Dynamic', unhex(concat('2e017801', repeat('0d01', 300000)))) SETTINGS input_format_binary_max_type_complexity = 0; -- { serverError TOO_DEEP_RECURSION }
SELECT finalizeAggregation(CAST(unhex(concat('012e017801', repeat('0d01', 300000))) AS AggregateFunction(any, Dynamic))); -- { serverError TOO_DEEP_RECURSION }

-- Ordinary parameters still fit in the budget: AggregateFunction(quantiles(0.1, 0.9), UInt64).
SELECT dynamicType(d), finalizeAggregation(d::AggregateFunction(quantiles(0.1, 0.9), UInt64))
FROM format(RowBinary, 'd Dynamic', unhex('2500097175616e74696c657302079a9999999999b93f07cdccccccccccec3f0104002000000000000003000000000000001c36333634313336323233383436373933303035203020313233343539000000000000000001000000000000000200000000000000'))
SETTINGS input_format_binary_max_type_complexity = 1000;
