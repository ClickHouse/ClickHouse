-- analysisOfVariance keeps three parallel arrays in its aggregate state (one entry per group).
-- The finalize path iterates one array and indexes the others at the same position, so a state
-- deserialized from raw bytes with mismatched array lengths reads out of bounds. Such a state is
-- reachable from any user via CAST of a String to AggregateFunction.

-- Normal aggregation builds equal-length arrays and must keep working.
SELECT analysisOfVariance(number, number % 3) FROM numbers(30) FORMAT Null;

-- xs1 and xs2 have two groups, ns has one -> ns is read past its end.
SELECT finalizeAggregation(CAST(unhex('02000000000000f03f000000000000f03f02000000000000f03f000000000000f03f010500000000000000') AS AggregateFunction(analysisOfVariance, Float64, UInt8))); -- { serverError INCORRECT_DATA }
