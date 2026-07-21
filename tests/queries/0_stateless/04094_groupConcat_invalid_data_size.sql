-- Verify that groupConcat deserialize rejects absurdly large data_size values.
-- The fuzzer found that CAST(unhex(...), 'AggregateFunction(groupConcat, ...)') with
-- garbage binary data could decode an enormous VarUInt for data_size, hitting a
-- LOGICAL_ERROR in the allocator (fatal in sanitizer builds) instead of a clean
-- user-facing exception.

-- VarUInt 80808080808080808001 decodes to 2^63, directly at the allocator's
-- 0x8000000000000000 LOGICAL_ERROR threshold.
SELECT finalizeAggregation(CAST(unhex('80808080808080808001'), 'AggregateFunction(groupConcat, String)')); -- { serverError BAD_ARGUMENTS }
SELECT finalizeAggregation(CAST(unhex('80808080808080808001'), 'AggregateFunction(groupConcat(\',\', 10), String)')); -- { serverError BAD_ARGUMENTS }

-- VarUInt FFFFFFFFFFFFFFFF7F decodes to 0x7FFFFFFFFFFFFFFF, which is just below
-- the allocator's 0x8000000000000000 threshold. A naive check at that exact
-- threshold would still trigger LOGICAL_ERROR after Arena adds PADDING_FOR_SIMD
-- and page-aligns the request upward.
SELECT finalizeAggregation(CAST(unhex('FFFFFFFFFFFFFFFF7F'), 'AggregateFunction(groupConcat, String)')); -- { serverError BAD_ARGUMENTS }
SELECT finalizeAggregation(CAST(unhex('FFFFFFFFFFFFFFFF7F'), 'AggregateFunction(groupConcat(\',\', 10), String)')); -- { serverError BAD_ARGUMENTS }

-- Pin the exact cap boundary (2^48).
-- Just below/at cap: passes the check, then fails cleanly via the memory tracker
-- or malloc — the point is no LOGICAL_ERROR.
SELECT finalizeAggregation(CAST(unhex('80808080808040'), 'AggregateFunction(groupConcat, String)')); -- { serverError MEMORY_LIMIT_EXCEEDED, CANNOT_ALLOCATE_MEMORY }
-- Just above cap: caught by the check.
SELECT finalizeAggregation(CAST(unhex('81808080808040'), 'AggregateFunction(groupConcat, String)')); -- { serverError BAD_ARGUMENTS }

-- Original fuzzer-found case that triggered LOGICAL_ERROR in the allocator:
SELECT CAST(unhex(toFixedString('', 30)), 'AggregateFunction(groupConcat(\',\', 10), String)'); -- { serverError BAD_ARGUMENTS }
