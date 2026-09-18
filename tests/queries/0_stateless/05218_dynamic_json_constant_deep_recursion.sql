SELECT finalizeAggregation(CAST(unhex('0130008008200000000101613000800820000000010161300080082000000000') AS AggregateFunction(any, Dynamic)));

-- Turning a Dynamic constant into a literal recurses once per JSON level, and the depth comes from the
-- value, so it must be reported rather than overflow the stack.
SELECT finalizeAggregation(CAST(unhex(concat('01', repeat('3000800820000000010161', 4000), '300080082000000000')) AS AggregateFunction(any, Dynamic))); -- { serverError TOO_DEEP_RECURSION }
