-- https://github.com/ClickHouse/ClickHouse/issues/102211
-- reinterpret() to Array(LowCardinality(...)) must be rejected at validation
-- time with ILLEGAL_TYPE_OF_ARGUMENT, not at runtime with NOT_IMPLEMENTED.

SELECT reinterpret(x'01020304', 'Array(LowCardinality(Int32))'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT reinterpret(x'01020304', 'Array(LowCardinality(UInt32))'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT reinterpret(x'0102030405060708', 'Array(LowCardinality(Float64))'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT reinterpret(x'0102030405060708', 'Array(LowCardinality(FixedString(4)))'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Sanity: non-LowCardinality Array(Int32) still works.
SELECT reinterpret(x'01020304', 'Array(Int32)');
