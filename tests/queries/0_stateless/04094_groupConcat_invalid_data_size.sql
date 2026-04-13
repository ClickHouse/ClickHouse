-- Verify that groupConcat deserialize rejects data_size values that are too large.
-- This was found by the fuzzer: CAST(unhex(...), 'AggregateFunction(groupConcat, ...)') with
-- garbage binary data could decode an enormous VarUInt for data_size, causing a LOGICAL_ERROR
-- in the allocator instead of a clean user-facing exception.

-- Without the has_limit path (no parameters to groupConcat):
-- VarUInt 0x80808008 decodes to 16777216 (0x1000000), exceeding the 0xFFFFFF limit.
SELECT finalizeAggregation(CAST(unhex('80808008'), 'AggregateFunction(groupConcat, String)')); -- { serverError BAD_ARGUMENTS }

-- With the has_limit path (groupConcat with delimiter and limit):
SELECT finalizeAggregation(CAST(unhex('80808008'), 'AggregateFunction(groupConcat(\',\', 10), String)')); -- { serverError BAD_ARGUMENTS }
