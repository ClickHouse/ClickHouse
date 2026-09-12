-- analysisOfVariance and largestTriangleThreeBuckets keep parallel arrays in their
-- aggregate state (one entry per group / one (x, y) point). The finalize path iterates
-- one array and indexes the others at the same position, so a state deserialized from
-- raw bytes with mismatched array lengths reads out of bounds. Such a state is reachable
-- from any user via CAST of a String to AggregateFunction.

-- Normal aggregation builds equal-length arrays and must keep working.
SELECT analysisOfVariance(number, number % 3) FROM numbers(30) FORMAT Null;
SELECT largestTriangleThreeBuckets(5)(number, number * 2) FROM numbers(30) FORMAT Null;

-- analysisOfVariance: xs1 and xs2 have two groups, ns has one -> ns is read past its end.
SELECT finalizeAggregation(CAST(unhex('02000000000000f03f000000000000f03f02000000000000f03f000000000000f03f010500000000000000') AS AggregateFunction(analysisOfVariance, Float64, UInt8))); -- { serverError INCORRECT_DATA }

-- largestTriangleThreeBuckets: x holds two points, y holds one -> y is read past its end.
SELECT finalizeAggregation(CAST(unhex('0201000000000000f03f00000000000000400000000000000840') AS AggregateFunction(largestTriangleThreeBuckets(100), Float64, Float64))); -- { serverError INCORRECT_DATA }
