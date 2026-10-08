-- The spelling of a number literal decides its value, the other arguments decide its type. Next to a
-- float an integer too large for UInt64 is a float. In arithmetic with a Decimal a literal that is not
-- a plain Int64/UInt64 stays a Float64, so the result is a Float64 as with any other float.

SELECT 'float', toFloat64(number) + 100000000000000000000000 AS r, toTypeName(r) FROM numbers(1);
SELECT 'float', toFloat32(number) * 100000000000000000000000 AS r, toTypeName(r) FROM numbers(1);
SELECT 'float', greatest(toFloat32(number), toFloat64(number), 100000000000000000000000) AS r, toTypeName(r) FROM numbers(1);
SELECT 'float', greatest(toFloat64(number), toFloat32(number), 100000000000000000000000) AS r, toTypeName(r) FROM numbers(1);

SELECT 'decimal', toDecimal64(number, 2) + 100000000000000000000000 AS r, toTypeName(r) FROM numbers(1);
SELECT 'decimal', toDecimal64(number + 1, 2) * 1.1 AS r, toTypeName(r) FROM numbers(1);
SELECT 'decimal', toDecimal64(number + 1, 2) * 1.10 AS r, toTypeName(r) FROM numbers(1);
SELECT 'decimal', toDecimal64(number + 1, 2) + 1.123456789012345678 AS r, toTypeName(r) FROM numbers(1);
SELECT 'decimal', toDecimal32(number + 1, 2) + 0.0015 AS r, toTypeName(r) FROM numbers(1);
SELECT 'decimal', toDecimal32(number + 1, 2) + 1.5e-3 AS r, toTypeName(r) FROM numbers(1);
SELECT 'decimal', toDecimal128(number + 1, 2) - 1e5 AS r, toTypeName(r) FROM numbers(1);
SELECT 'decimal', greatest(toDecimal64(number, 2), 1.5) AS r, toTypeName(r) FROM numbers(1);

SELECT 'type', toTypeName(toDecimal32(number, 5) + 1.5), toTypeName(toDecimal32(number, 2) + 1.123), toTypeName(toDecimal64(number, 2) + 100000000000000000000000) FROM numbers(1);

-- Division is a float division: it keeps the fractional digits and a division by zero is inf.
SELECT 'divide', toDecimal32(number + 1, 5) / 1.5 AS r, toTypeName(r) FROM numbers(1);
SELECT 'divide', toDecimal64(number + 1, 5) / 1.5 AS r, toTypeName(r) FROM numbers(1);
SELECT 'divide', 1.5 / toDecimal64(number + 1, 5) AS r, toTypeName(r) FROM numbers(1);
SELECT 'divide', 1.5 / toDecimal32(number + 1, 5) AS r, toTypeName(r) FROM numbers(1);
SELECT 'divide', toDecimal32(number + 1, 5) / 0.0 AS r, toTypeName(r) FROM numbers(1);
SELECT 'divide', toDecimal64(number + 1, 5) / 0.0 AS r, toTypeName(r) FROM numbers(1);

SELECT 'decimal', toTypeName(toDecimal64(number, 2) + 1e80) FROM numbers(1);

-- The integer family still widens exactly.
SELECT 'int', toInt64(number) + 100000000000000000000000 AS r, toTypeName(r) FROM numbers(1);
