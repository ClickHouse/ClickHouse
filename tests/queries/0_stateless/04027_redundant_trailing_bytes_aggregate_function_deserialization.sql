SELECT finalizeAggregation(CAST(unhex('00DEADBEEF'), 'AggregateFunction(studentTTest, Nullable(Float64), Nullable(UInt8))')); -- { serverError BAD_ARGUMENTS }

SELECT finalizeAggregation(CAST(unhex('2A00000000000000DEADBEEF'), 'AggregateFunction(sum, UInt64)')); -- { serverError BAD_ARGUMENTS }

SELECT finalizeAggregation(CAST(concat(CAST(sumState(toUInt64(42)), 'String'), unhex('AA')), 'AggregateFunction(sum, UInt64)')); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS test;
CREATE TABLE test (s AggregateFunction(sum, UInt64)) ENGINE = Memory;
SET aggregate_function_input_format = 'state';
INSERT INTO test SELECT concat(CAST(sumState(toUInt64(42)), 'String'), unhex('AA')); -- { serverError BAD_ARGUMENTS }
