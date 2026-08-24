CREATE TABLE mytable
(
    operand Float64,
    low     Float64,
    high     Float64,
    count   UInt64,
    PRIMARY KEY (operand, low, high, count)
) ENGINE = MergeTree();

INSERT INTO mytable VALUES (3, -100, 200, 10), (0, 0, 10, 4), (3, 0, 10, 3), (4.333, 1, 11, 3), (4.34, 1, 11, 3), (-7.6, -10, 0, 4), (-6, -5, -1, 2), (1, 3, 0, 1), (3, 2, 5, 0);

SELECT operand, low, high, count, WIDTH_BUCKET(operand, low, high, count) FROM mytable WHERE count != 0;
SELECT '----------';
-- zero is not valid for count
SELECT operand, low, high, count, WIDTH_BUCKET(operand, low, high, count) FROM mytable WHERE count = 0; -- { serverError BAD_ARGUMENTS }
-- operand, low and high cannot be NaN
SELECT WIDTH_BUCKET(0, 10, NaN, 10); -- { serverError BAD_ARGUMENTS }
SELECT WIDTH_BUCKET(NaN, 0, 10, 10); -- { serverError BAD_ARGUMENTS }
SELECT WIDTH_BUCKET(0, NaN, 10, 10); -- { serverError BAD_ARGUMENTS }
-- low and high cannot be Inf
SELECT WIDTH_BUCKET(1, -Inf, 10, 10); -- { serverError BAD_ARGUMENTS }
-- low and high cannot be Inf
SELECT WIDTH_BUCKET(1, 0, Inf, 10); -- { serverError BAD_ARGUMENTS }
-- operand can be Inf
SELECT WIDTH_BUCKET(-Inf, 0, 10, 10);
SELECT WIDTH_BUCKET(Inf, 0, 10, 10);
SELECT '----------';
-- IntXX types
SELECT toInt64(operand) AS operand, toInt32(low) AS low, toInt16(high) AS high, count, WIDTH_BUCKET(operand, low, high, count) FROM mytable WHERE count != 0;
SELECT '----------';
-- UIntXX types
SELECT toUInt8(toInt8(operand)) AS operand, toUInt16(toInt16(low)) AS low, toUInt32(toInt32(high)) AS high, count, WIDTH_BUCKET(operand, low, high, count) FROM mytable WHERE count != 0;
SELECT '----------';
SELECT WIDTH_BUCKET(1, 2, 3, -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT WIDTH_BUCKET(1, 2, 3, 1.3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT WIDTH_BUCKET('a', 1, 2, 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT WIDTH_BUCKET(1, toUInt128(42), 2, 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT WIDTH_BUCKET(1, 2, toInt128(42), 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT WIDTH_BUCKET(1, 2, 3, toInt256(42)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT '----------';
-- Return type checks
SELECT toTypeName(WIDTH_BUCKET(1, 2, 3, toUInt8(1)));
SELECT toTypeName(WIDTH_BUCKET(1, 2, 3, toUInt16(1)));
SELECT toTypeName(WIDTH_BUCKET(1, 2, 3, toUInt32(1)));
SELECT toTypeName(WIDTH_BUCKET(1, 2, 3, toUInt64(1)));
SELECT '----------';
-- Test handling ColumnConst
SELECT WIDTH_BUCKET(1, low, high, count) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(operand, 2, high, count) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(3, 3, high, count) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(operand, low, 4, count) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(5, low, 5, count) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(operand, 6, 6, count) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(7, 7, 7, count) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(operand, low, high, 8) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(9, low, high, 9) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(operand, 10, high, 10) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(11, 11, high, 11) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(operand, low, 12, 12) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(13, low, 13, 13) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(operand, 14, 14, 14) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(15, 15, 15, 15) FROM mytable WHERE count != 0;