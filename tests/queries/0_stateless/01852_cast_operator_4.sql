SET enable_analyzer = 1;

SELECT [3,4,5][1]::Int32;
EXPLAIN SYNTAX SELECT [3,4,5][1]::Int32;

SELECT [3,4,5]::Array(Int64)[2]::Int8;
EXPLAIN SYNTAX SELECT [3,4,5]::Array(Int64)[2]::Int8;

SELECT [1,2,3]::Array(UInt64)[[number, number]::Array(UInt8)[number]::UInt64]::UInt8 from numbers(3);
EXPLAIN SYNTAX SELECT [1,2,3]::Array(UInt64)[[number, number]::Array(UInt8)[number]::UInt64]::UInt8 from numbers(3);

WITH [3,4,5] AS x SELECT x[1]::Int32;
EXPLAIN SYNTAX WITH [3,4,5] AS x SELECT x[1]::Int32;

SELECT tuple(3,4,5).1::Int32;
EXPLAIN SYNTAX SELECT tuple(3,4,5).1::Int32;

SELECT tuple(3,4,5)::Tuple(UInt64, UInt64, UInt64).2::Int32;
EXPLAIN SYNTAX SELECT tuple(3,4,5)::Tuple(UInt64, UInt64, UInt64).1::Int32;

WITH tuple(3,4,5) AS x SELECT x.1::Int32;
EXPLAIN SYNTAX WITH tuple(3,4,5) AS x SELECT x.1::Int32;
