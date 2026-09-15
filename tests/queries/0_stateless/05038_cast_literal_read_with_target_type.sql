-- `CAST` reads a number written directly as its argument with the target type, whenever the target
-- type reads the digits more precisely than the number itself carries them: a `Decimal`, or an
-- integer wider than 64 bits. Every other type keeps reading the number, because for those the text
-- is either no more precise or means something else entirely - `1` is one second past the epoch for
-- `DateTime`, but a calendar date as text.
-- https://github.com/ClickHouse/ClickHouse/issues/116025

SET session_timezone = 'UTC';

-- All the ways of writing the cast agree, and keep every digit.
SELECT CAST(0.1 AS Decimal256(76));
SELECT CAST(0.1, 'Decimal256(76)');
SELECT 0.1::Decimal256(76);
SELECT CAST('0.1' AS Decimal256(76));
SELECT CAST(0.1 AS Decimal256(76)) = CAST('0.1' AS Decimal256(76));

SELECT CAST(1.1 AS Decimal(30, 20)), CAST(-1.1 AS Decimal(30, 20));
SELECT CAST(1e-30 AS Decimal256(40)), CAST(1e3 AS Decimal32(2));
SELECT CAST(0.1 AS DEC(76, 76)), CAST(0.1 AS NUMERIC(76, 76));

-- An integer of more than 19 digits used to reach the wide integers through `Float64`.
SELECT CAST(607668569663131286404589520 AS UInt128);
SELECT CAST(-607668569663131286404589520 AS Int128);
SELECT CAST(123456789012345678901234567890 AS Decimal256(0));

-- Through the wrappers that hand the text over to the type they wrap.
SELECT CAST(0.1 AS Nullable(Decimal256(76)));
SELECT CAST([0.1, 0.2] AS Array(Decimal256(76)));
SELECT CAST([[0.1]] AS Array(Array(Decimal256(76))));

-- An alias on the argument does not get in the way.
SELECT CAST(0.1 AS x, 'Decimal256(76)');
SELECT CAST(0.1 AS x, 'Decimal256(76)') AS y;

-- The exact value survives the constant-expression template of `Values`.
CREATE TEMPORARY TABLE t (d Decimal256(76));
INSERT INTO t VALUES (CAST(0.1 AS Decimal256(76))), (CAST(0.2 AS Decimal256(76)));
SELECT * FROM t ORDER BY d;

-- Only a whole number written as the argument is read with the type; an expression is not.
SELECT CAST(0.1 + 0 AS Decimal256(76));
SELECT CAST((0.1) AS Decimal256(76));
SELECT CAST(-(0.1) AS Decimal256(76));
SELECT CAST(materialize(0.1) AS Decimal256(76));
SELECT CAST(0.1, concat('Decimal', '256(76)'));

-- Types that read the text as something other than the number keep reading the number.
SELECT CAST(1 AS DateTime('UTC')), CAST(1234567890 AS DateTime('UTC'));
SELECT CAST(19000 AS Date), CAST(1234567890 AS Date32);
SELECT CAST(-1 AS UInt8), CAST(1000 AS UInt8), CAST(1.9 AS UInt8);
-- `1e18` rather than a number over the `Int64` range: converting an out-of-range `Float64` to an
-- integer is undefined, and the two architectures give different answers.
SELECT CAST(1.9 AS Int64), CAST(1e18 AS Int64);
SELECT CAST(42 AS Bool), CAST(1 AS Enum8('a' = 1, 'b' = 2)), CAST(1234567890 AS IPv4);
SELECT CAST(1.0 AS String), CAST(1e3 AS String);
SELECT CAST(256 AS Nullable(UInt8));

-- The wide integers only read an integer written without an exponent, and the unsigned ones only a
-- non-negative one; the rest keeps being read as a number.
SELECT CAST(-1 AS UInt128), CAST(1.9 AS Int128), CAST(1e19 AS UInt256);
SELECT CAST(-1 AS Int256), CAST(1 AS UInt128);

-- A number no type reads as text - hexadecimal, binary, or with digit separators - is read as a
-- number whatever the target type is.
SELECT CAST(0xFF AS UInt8), CAST(0b101 AS UInt8), CAST(1_000 AS UInt16);
SELECT CAST(0xFF AS Decimal32(2)), CAST(1_000 AS Decimal32(2)), CAST(0b101 AS UInt128);

-- A number the target type cannot hold now raises, where reading it as a `Float64` and converting
-- returned a wrong value: `Decimal32(9)` carries scale 9 at precision 9, so it cannot hold `1`.
SELECT CAST(1 AS Decimal32(9)); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- A string inside a collection is not part of a numeral, so the collection is not read as text.
SELECT CAST(['0.1'] AS Array(Decimal32(2))), CAST(['1'] AS Array(Int128));

-- The `::` operator used to hand the text of every number to the type, including the forms no type
-- reads back.
SELECT 0x10::UInt8, 0b101::UInt8, 1_000::UInt16, [0xFF]::Array(UInt8);

-- A round bracket holding a single number or string is a parenthesized expression, not a one-element
-- tuple; the `::` operator used to take its text and hand it to a type that does not read tuples.
SELECT (1)::UInt8, (0.1)::Decimal32(2), ('a')::String;
-- Anything else a round bracket can hold is a tuple, and its text is still read as one.
SELECT (1, 2)::Tuple(UInt8, UInt8), ()::Tuple(Dynamic), (())::Tuple(Tuple(Dynamic));
SELECT [0.1]::Array(Decimal32(2)), (1 + 2)::UInt32;

EXPLAIN SYNTAX SELECT CAST(0.1 AS Decimal256(76));
EXPLAIN SYNTAX SELECT CAST(0.1, 'Decimal256(76)');
EXPLAIN SYNTAX SELECT CAST(0.1 AS Float64);
EXPLAIN SYNTAX SELECT CAST(1 AS DateTime);
EXPLAIN SYNTAX SELECT CAST([0.1] AS Array(Decimal32(2)));
