-- Tags: no-fasttest
-- - no-fasttest -- compiled w/o datasketches

-- Apache DataSketches can serialize a Theta sketch in a compressed form (serialization
-- version 4) that packs the retained values at a variable number of bits per entry.
-- ClickHouse always writes the uncompressed form, but it has to read the compressed one,
-- because an `AggregateFunction(uniqTheta, ...)` state can come from another
-- implementation of the library. Two of the unpacking routines decoded it incorrectly,
-- and the result was a silently too low estimate rather than an error.

-- Both states below are canonical compressed encodings of a sketch that retains 16 values
-- with theta = 1, so the estimate is exact. The first packs its values at 33 bits per
-- entry and the second at 35; the two widths are decoded by separate routines. The two
-- sketches have exactly two values in common, so their union retains 30.

WITH
    CAST(unhex('4B01040321011ACC9310000001F4000000FA0000007D0000003E8000001F4000000FA0000007D180000000000001F4000000FA0000007D0000003E8000001F4000000FA0000007D0000003E8') AS AggregateFunction(uniqTheta, UInt64)) AS state_33_bits,
    CAST(unhex('4F01040323011ACC93100000007D0000000FA1000000004000000000000007D0000000FA0000001F40000003E80000007D0000000FA1000000000000003E80000007D0000000FA0000001F40000003E8') AS AggregateFunction(uniqTheta, UInt64)) AS state_35_bits
SELECT
    finalizeAggregation(state_33_bits),
    finalizeAggregation(state_35_bits),
    finalizeAggregation(uniqThetaUnion(state_33_bits, state_35_bits));
