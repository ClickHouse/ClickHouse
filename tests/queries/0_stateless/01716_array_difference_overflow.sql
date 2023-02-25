-- Overflow is Ok and behaves as the CPU does it.
SELECT arrayDifference([65536, -9223372036854775808]);
