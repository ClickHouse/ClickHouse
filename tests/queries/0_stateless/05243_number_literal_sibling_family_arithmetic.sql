-- The spelling of a number literal decides its value, the other arguments decide its type. Next to a
-- float an integer too large for UInt64 is a float. Next to a Decimal any literal is cast to the
-- sibling's Decimal type, widened only where it does not fit, and then behaves exactly like an
-- explicit constant of that type would, limits of Decimal arithmetic included.

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

-- The literal takes the sibling's type when it fits it, and is widened only as far as needed.
SELECT 'type', toTypeName(toDecimal32(number, 5) + 1.5), toTypeName(toDecimal32(number, 2) + 1.123), toTypeName(toDecimal64(number, 2) + 100000000000000000000000) FROM numbers(1);

-- Division follows the same Decimal rules as with an explicit constant: `1.5` is `Decimal32(5)` next to a
-- `Decimal32(5)`, and that division overflows Int32 either way.
SELECT 'divide', toDecimal32(number + 1, 5) / 1.5 FROM numbers(1); -- { serverError DECIMAL_OVERFLOW }
SELECT 'divide', toDecimal64(number + 1, 5) / 1.5 AS r, toTypeName(r) FROM numbers(1);
SELECT 'divide', 1.5 / toDecimal64(number + 1, 5) AS r, toTypeName(r) FROM numbers(1);
SELECT 'divide', 1.5 / toDecimal32(number + 1, 5) FROM numbers(1); -- { serverError DECIMAL_OVERFLOW }
SELECT 'divide', toDecimal32(number + 1, 5) / 0.0 FROM numbers(1); -- { serverError DECIMAL_OVERFLOW }
SELECT 'divide', toDecimal64(number + 1, 5) / 0.0 FROM numbers(1); -- { serverError ILLEGAL_DIVISION }

-- A literal that does not fit Decimal256 still goes through Float64.
SELECT 'decimal', toTypeName(toDecimal64(number, 2) + 1e80) FROM numbers(1);

-- The integer family still widens exactly.
SELECT 'int', toInt64(number) + 100000000000000000000000 AS r, toTypeName(r) FROM numbers(1);
