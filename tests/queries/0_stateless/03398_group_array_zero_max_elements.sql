-- Related to https://github.com/ClickHouse/ClickHouse/issues/78088

-- Asserting that groupArray* function calls with zero `max_size` argument of
-- different types (Int/UInt) will produce BAD_ARGUMENTS error

SELECT groupArray(0::UInt64)(1); -- { serverError BAD_ARGUMENTS }
SELECT groupArray(0::Int64)(1); -- { serverError BAD_ARGUMENTS }
SELECT groupArray(0::UInt64)('x'); -- { serverError BAD_ARGUMENTS }
SELECT groupArray(0::Int64)('x'); -- { serverError BAD_ARGUMENTS }
SELECT groupArray(0::UInt64)(number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }
SELECT groupArray(0::Int64)(number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }

SELECT groupArraySorted(0::UInt64)(1); -- { serverError BAD_ARGUMENTS }
SELECT groupArraySorted(0::Int64)(1); -- { serverError BAD_ARGUMENTS }
SELECT groupArraySorted(0::UInt64)('x'); -- { serverError BAD_ARGUMENTS }
SELECT groupArraySorted(0::Int64)('x'); -- { serverError BAD_ARGUMENTS }
SELECT groupArraySorted(0::UInt64)(number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }
SELECT groupArraySorted(0::Int64)(number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }

SELECT groupArraySample(0::UInt64, 123)(1); -- { serverError BAD_ARGUMENTS }
SELECT groupArraySample(0::Int64, 123)(1); -- { serverError BAD_ARGUMENTS }
SELECT groupArraySample(0::UInt64, 123)('x'); -- { serverError BAD_ARGUMENTS }
SELECT groupArraySample(0::Int64, 123)('x'); -- { serverError BAD_ARGUMENTS }
SELECT groupArraySample(0::UInt64, 123)(number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }
SELECT groupArraySample(0::Int64, 123)(number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }

SELECT groupArrayLast(0::UInt64)(1); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayLast(0::Int64)(1); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayLast(0::UInt64)('x'); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayLast(0::Int64)('x'); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayLast(0::UInt64)(number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayLast(0::Int64)(number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }

SELECT groupArrayMovingSum(0::UInt64)(1); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayMovingSum(0::Int64)(1); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayMovingSum(0::UInt64)('x'); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayMovingSum(0::Int64)('x'); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayMovingSum(0::UInt64)(number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayMovingSum(0::Int64)(number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }

SELECT groupArrayMovingAvg(0::UInt64)(1); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayMovingAvg(0::Int64)(1); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayMovingAvg(0::UInt64)('x'); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayMovingAvg(0::Int64)('x'); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayMovingAvg(0::UInt64)(number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayMovingAvg(0::Int64)(number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }

SELECT groupUniqArray(0::UInt64)(1); -- { serverError BAD_ARGUMENTS }
SELECT groupUniqArray(0::Int64)(1); -- { serverError BAD_ARGUMENTS }
SELECT groupUniqArray(0::UInt64)('x'); -- { serverError BAD_ARGUMENTS }
SELECT groupUniqArray(0::Int64)('x'); -- { serverError BAD_ARGUMENTS }
SELECT groupUniqArray(0::UInt64)(number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }
SELECT groupUniqArray(0::Int64)(number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }
