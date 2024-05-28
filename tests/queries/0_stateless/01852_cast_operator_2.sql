SELECT (0.1, 0.2)::Tuple(Decimal(75, 70), Decimal(75, 70));
EXPLAIN SYNTAX SELECT (0.1, 0.2)::Tuple(Decimal(75, 70), Decimal(75, 70)) SETTINGS allow_experimental_analyzer = 0;
EXPLAIN SYNTAX SELECT (0.1, 0.2)::Tuple(Decimal(75, 70), Decimal(75, 70)) SETTINGS allow_experimental_analyzer = 1;

SELECT 0.1 :: Decimal(4, 4);
EXPLAIN SYNTAX SELECT 0.1 :: Decimal(4, 4) SETTINGS allow_experimental_analyzer = 0;
EXPLAIN SYNTAX SELECT 0.1 :: Decimal(4, 4) SETTINGS allow_experimental_analyzer = 1;

SELECT [1, 2, 3] :: Array(Int32);
EXPLAIN SYNTAX SELECT [1, 2, 3] :: Array(Int32) SETTINGS allow_experimental_analyzer = 0;
EXPLAIN SYNTAX SELECT [1, 2, 3] :: Array(Int32) SETTINGS allow_experimental_analyzer = 1;

SELECT [1::UInt32, 2::UInt32]::Array(UInt64);
EXPLAIN SYNTAX SELECT [1::UInt32, 2::UInt32]::Array(UInt64) SETTINGS allow_experimental_analyzer = 0;
EXPLAIN SYNTAX SELECT [1::UInt32, 2::UInt32]::Array(UInt64) SETTINGS allow_experimental_analyzer = 1;

SELECT [[1, 2]::Array(UInt32), [3]]::Array(Array(UInt64));
EXPLAIN SYNTAX SELECT [[1, 2]::Array(UInt32), [3]]::Array(Array(UInt64)) SETTINGS allow_experimental_analyzer = 0;
EXPLAIN SYNTAX SELECT [[1, 2]::Array(UInt32), [3]]::Array(Array(UInt64)) SETTINGS allow_experimental_analyzer = 1;

SELECT [[1::UInt16, 2::UInt16]::Array(UInt32), [3]]::Array(Array(UInt64));
EXPLAIN SYNTAX SELECT [[1::UInt16, 2::UInt16]::Array(UInt32), [3]]::Array(Array(UInt64)) SETTINGS allow_experimental_analyzer = 0;
EXPLAIN SYNTAX SELECT [[1::UInt16, 2::UInt16]::Array(UInt32), [3]]::Array(Array(UInt64)) SETTINGS allow_experimental_analyzer = 1;

SELECT [(1, 'a'), (3, 'b')]::Nested(u UInt8, s String) AS t, toTypeName(t);
