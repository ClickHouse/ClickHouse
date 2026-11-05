-- Test that self-merge of NumericIndexedVector aggregate states does not trigger
-- assertion failure in CRoaring (x1 != x2 in `roaring_bitmap_xor_inplace`).
-- https://github.com/ClickHouse/ClickHouse/issues/99704

-- `multiply` triggers self-merge via exponentiation by squaring (even branch).
SELECT arrayJoin([numericIndexedVectorToMap(
    multiply(2, groupNumericIndexedVectorState(100, 1)))]);

-- Power of 2 forces multiple self-merge iterations.
SELECT arrayJoin([numericIndexedVectorToMap(
    multiply(4, groupNumericIndexedVectorState(100, 1)))]);
