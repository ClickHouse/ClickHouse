-- Regression test for UBSan signed-integer overflow at AggregateFunctionFlameGraph.cpp:418.
-- When `flameGraph(trace, size, ptr)` received `size = INT64_MIN`, the dealloc branch
-- computed `UInt64 abs_size = -size`, which is undefined behaviour because the
-- magnitude `2^63` is not representable in `Int64`. The fix casts to `UInt64`
-- before negating, matching the sibling allocation path.
-- STID: 3765-3f7f.

SET allow_introspection_functions = 1;

-- INT64_MIN as a deallocation size: must not trigger UBSan / abort the server.
-- The dealloc is unmatched (no prior allocation for ptr=42), so nothing is emitted.
SELECT arrayJoin(flameGraph(trace, sz, ptr)) FROM (
    SELECT CAST([18446744073709551600, 18446744073709551601] AS Array(UInt64)) AS trace,
           CAST(-9223372036854775808 AS Int64) AS sz,
           CAST(42 AS UInt64) AS ptr
);

-- Confirm follow-up queries still work after the bug query above.
SELECT 'after';
