-- Overflow is Ok and behaves as the CPU does it.
SELECT arrayDifference([65536, -9223372036854775808]);

-- Diff of unsigned int -> int
SELECT arrayDifference( cast([10, 1], 'Array(UInt8)'));
SELECT arrayDifference( cast([10, 1], 'Array(UInt16)'));
SELECT arrayDifference( cast([10, 1], 'Array(UInt32)'));
SELECT arrayDifference( cast([10, 1], 'Array(UInt64)'));
