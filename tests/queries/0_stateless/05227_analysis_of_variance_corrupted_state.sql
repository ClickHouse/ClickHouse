-- Deserialization of `analysisOfVariance` state must reject vectors of inconsistent sizes
-- instead of reading out of bounds in `merge` or finalization.

-- xs1.size() = 2, xs2.size() = 0, ns.size() = 1
SELECT finalizeAggregation(CAST(unhex('02000000000000f03f000000000000004000010100000000000000'), 'AggregateFunction(analysisOfVariance, Float64, UInt64)')); -- { serverError CORRUPTED_DATA }

-- xs1.size() = 1, xs2.size() = 1, ns.size() = 0
SELECT finalizeAggregation(CAST(unhex('01000000000000f03f01000000000000f03f00'), 'AggregateFunction(analysisOfVariance, Float64, UInt64)')); -- { serverError CORRUPTED_DATA }

-- A genuine state round-trips.
SELECT finalizeAggregation(CAST(unhex(hex(state)), 'AggregateFunction(analysisOfVariance, Float64, UInt64)')) = finalizeAggregation(state)
FROM (SELECT analysisOfVarianceState(number::Float64, number % 3) AS state FROM numbers(30));
