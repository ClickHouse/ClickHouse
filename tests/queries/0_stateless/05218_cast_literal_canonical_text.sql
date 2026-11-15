-- The text a literal argument of `CAST` is read with is put together from its tokens, not copied from
-- the query: the text readers of the types skip neither a comment nor a space between a minus and its
-- digits. The only spacing kept is a single space after a comma, when the query has one. A literal not written plainly - in brackets, in hexadecimal, with a leading plus - is read
-- with the type from the way it is written back into a query, so that formatting a query and parsing
-- it back gives the same query.
-- https://github.com/ClickHouse/ClickHouse/pull/116043

-- A space or a comment inside the literal.
SELECT CAST(- 1 AS Int128), CAST(-  0.5 AS Decimal32(2));
SELECT CAST([0.1 /* c */, 0.2] AS Array(Decimal256(76)));
SELECT CAST([ 1 , -- a comment to the end of the line
    - 2 ] AS Array(Int128));
SELECT - 1::Int128, [1 /* c */, - 2]::Array(Int128);
EXPLAIN SYNTAX SELECT CAST(- 1 AS Int128), CAST([0.1 /* c */, 0.2] AS Array(Decimal256(76))), - 1::Int128;
EXPLAIN SYNTAX SELECT CAST([1,2] AS Array(Int128)), CAST([ 1 ,
    2 ] AS Array(Int128)), [1,2]::Array(UInt8), [ 1 , 2 ]::Array(UInt8), [[1,2],[3]]::Array(Array(UInt8));

-- `-0` is the number `0`, which an unsigned type reads.
SELECT CAST(-0 AS UInt128), CAST([-0, -0.0] AS Array(Decimal32(2))), -0::UInt128, -0.0::Decimal32(2);
EXPLAIN SYNTAX SELECT CAST(-0 AS UInt128), -0::UInt128, CAST(-0.0 AS Decimal32(2));

-- A number in brackets, in hexadecimal, in binary, with digit separators, or with a leading plus is
-- the number written back in decimal.
SELECT CAST((0.1) AS Decimal256(76)), (0.1)::Decimal256(76);
SELECT CAST(0xFF AS Decimal32(2)), CAST(1_000 AS Decimal32(2)), CAST(0b101 AS UInt128), CAST(-0xFF AS Int128);
SELECT 0xFF::UInt128, 1_000::Decimal32(2), -0xFF::Int128;
SELECT CAST(+1 AS Int128), CAST([+1, +2] AS Array(UInt256)), +1::Int128;
SELECT CAST(0x1p3 AS Decimal32(2)), CAST((1e3) AS Decimal32(2));
EXPLAIN SYNTAX SELECT CAST((0.1) AS Decimal256(76)), CAST(0xFF AS Decimal32(2)), CAST(0b101 AS UInt128), 0xFF::UInt128, CAST(+1 AS Int128), CAST(0x1p3 AS Decimal32(2));

-- Written back, the number in brackets is the nearest `Float64`, which is all a number that is not
-- written plainly carries. Written plainly it keeps every digit.
SELECT CAST((0.10000000000000000000001) AS Decimal256(76)), CAST(0.10000000000000000000001 AS Decimal256(76));

-- A number written back in a form the type does not read - `1e19` is written back as `1e19`, and the
-- wide integers only read an integer - is still read as a number, as is a literal of another kind.
SELECT CAST((1e19) AS UInt256), CAST(0xFF AS UInt8), CAST((inf) AS Float64), CAST(true AS Int128);
EXPLAIN SYNTAX SELECT CAST((1e19) AS UInt256), CAST(0xFF AS UInt8), CAST(inf AS Decimal32(2)), CAST(true AS Int128);

-- Formatting a query and parsing it back gives the same query.
SELECT formatQuerySingleLine('SELECT CAST(- 1 AS Int128), CAST([0.1 /* c */, 0.2] AS Array(Decimal256(76))), CAST(0xFF AS Decimal32(2)), CAST(0b101 AS UInt128), CAST((0.1) AS Decimal256(76)), CAST(+1 AS Int128), CAST(-0 AS UInt128), CAST((-0.0) AS Decimal32(2)), 0xFF::UInt128, (0.1)::Decimal32(2), -0::UInt128, CAST(inf AS Decimal32(2)), CAST(0xFF AS UInt8), CAST([1, NULL] AS Array(UInt8))') AS formatted,
    formatQuerySingleLine(formatted) = formatted;

-- A minus in front of a fractional zero is kept: it is the floating-point negative zero. A minus in
-- front of an integer zero is dropped: it is the integer `0`.
SELECT -0.0::Float64, -0e0::Float64, CAST(-0.0 AS Float64), (-0.0::BFloat16), -0::Float64, -0::UInt8, CAST(-0.0 AS Decimal32(2));
SELECT formatQuerySingleLine('SELECT -0.0::Float64, CAST([-0.0, -0] AS Array(Decimal32(2))), CAST((-0.0) AS Decimal32(2)), -0::UInt8') AS formatted,
    formatQuerySingleLine(formatted) = formatted;
