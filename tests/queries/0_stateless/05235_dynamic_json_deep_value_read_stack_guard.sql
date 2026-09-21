-- A two-level value still decodes and prints, so the guard does not change ordinary nesting.
SELECT countIf(position(explain, 'constant_value: Object_((a, Object_((a, Object_()))))') > 0)
FROM (EXPLAIN QUERY TREE SELECT finalizeAggregation(CAST(unhex('0130008008200000000101613000800820000000010161300080082000000000') AS AggregateFunction(any, Dynamic))));

-- 4000 levels of Object/Dynamic/Variant nesting. The depth comes from the value rather than from the
-- declared type, so reading the value must be reported instead of exhausting the stack.
EXPLAIN QUERY TREE SELECT finalizeAggregation(CAST(unhex(concat('01', repeat('3000800820000000010161', 4000), '300080082000000000')) AS AggregateFunction(any, Dynamic))); -- { serverError TOO_DEEP_RECURSION }

-- Printing a value descends the same nesting through the text serialization of Dynamic, which every text
-- format of the type shares.
SELECT toString(finalizeAggregation(CAST(unhex(materialize('0130008008200000000101613000800820000000010161300080082000000000')) AS AggregateFunction(any, Dynamic))));

SELECT length(toString(finalizeAggregation(CAST(unhex(materialize(concat('01', repeat('3000800820000000010161', 4000), '300080082000000000'))) AS AggregateFunction(any, Dynamic))))); -- { serverError TOO_DEEP_RECURSION }
