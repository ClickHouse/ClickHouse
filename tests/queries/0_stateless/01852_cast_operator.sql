SELECT 0.1::Decimal(38, 38) AS c;
EXPLAIN SYNTAX SELECT 0.1::Decimal(38, 38) AS c;

SELECT [1, 2, 3]::Array(UInt32) AS c;
EXPLAIN SYNTAX SELECT [1, 2, 3]::Array(UInt32) AS c;

SELECT 'abc'::FixedString(3) AS c;
EXPLAIN SYNTAX SELECT 'abc'::FixedString(3) AS c;

SELECT 123::String AS c;
EXPLAIN SYNTAX SELECT 123::String AS c;

SELECT 1::Int8 AS c;
EXPLAIN SYNTAX SELECT 1::Int8 AS c;

SELECT [1, 1 + 1, 1 + 2]::Array(UInt32) AS c;
EXPLAIN SYNTAX SELECT [1, 1 + 1, 1 + 2]::Array(UInt32) AS c;

SELECT '2010-10-10'::Date AS c;
EXPLAIN SYNTAX SELECT '2010-10-10'::Date AS c;

SELECT '2010-10-10'::DateTime AS c;
EXPLAIN SYNTAX SELECT '2010-10-10'::DateTime AS c;

SELECT ['2010-10-10', '2010-10-10']::Array(Date) AS c;
EXPLAIN SYNTAX SELECT ['2010-10-10', '2010-10-10']::Array(Date);

SELECT (1 + 2)::UInt32 AS c;
EXPLAIN SYNTAX SELECT (1 + 2)::UInt32 AS c;

SELECT (0.1::Decimal(4, 4) * 5)::Float64 AS c;
EXPLAIN SYNTAX SELECT (0.1::Decimal(4, 4) * 5)::Float64 AS c;

SELECT number::UInt8 AS c, toTypeName(c) FROM numbers(1);
EXPLAIN SYNTAX SELECT number::UInt8 AS c, toTypeName(c) FROM numbers(1);

SELECT (0 + 1 + 2 + 3 + 4)::Date AS c;
EXPLAIN SYNTAX SELECT (0 + 1 + 2 + 3 + 4)::Date AS c;

SELECT (0.1::Decimal(4, 4) + 0.2::Decimal(4, 4) + 0.3::Decimal(4, 4))::Decimal(4, 4) AS c;
EXPLAIN SYNTAX SELECT (0.1::Decimal(4, 4) + 0.2::Decimal(4, 4) + 0.3::Decimal(4, 4))::Decimal(4, 4) AS c;

SELECT [[1][1]]::Array(UInt32);
SELECT [[1, 2, 3], [], [1]]::Array(Array(UInt32));
SELECT [[], []]::Array(Array(UInt32));
