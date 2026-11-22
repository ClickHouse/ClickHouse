-- `toDecimalString` rounds to the requested number of fractional digits. When every requested digit rounds up,
-- the carry leaves the fractional part and has to reach the whole part.

SELECT 'a carry out of the fractional part';
SELECT toDecimalString(toDecimal32('9.995', 3), 2), toDecimalString(toDecimal32('-9.995', 3), 2);
SELECT toDecimalString(toDecimal64('9.995', 3), 2), toDecimalString(toDecimal64('-9.995', 3), 2);
SELECT toDecimalString(toDecimal128('9.995', 3), 2), toDecimalString(toDecimal128('-9.995', 3), 2);
SELECT toDecimalString(toDecimal256('9.995', 3), 2), toDecimalString(toDecimal256('-9.995', 3), 2);

SELECT 'the carry cascades through the whole part';
SELECT toDecimalString(toDecimal32('99.95', 2), 1), toDecimalString(toDecimal32('-99.95', 2), 1);
SELECT toDecimalString(toDecimal64('99999999.9999999995', 10), 9);
SELECT toDecimalString(toDecimal128('99.9999', 4), 2), toDecimalString(toDecimal256('-99.9999', 4), 2);

SELECT 'a carry into a zero whole part';
SELECT toDecimalString(toDecimal32('0.995', 3), 2), toDecimalString(toDecimal32('-0.995', 3), 2);
SELECT toDecimalString(toDecimal64('0.5', 1), 0), toDecimalString(toDecimal256('-0.5', 2), 0);

SELECT 'the value rounds down, so the whole part is untouched';
SELECT toDecimalString(toDecimal32('9.994', 3), 2), toDecimalString(toDecimal32('-9.994', 3), 2);
SELECT toDecimalString(toDecimal64('0.004', 3), 2), toDecimalString(toDecimal64('-0.004', 3), 2);
SELECT toDecimalString(toDecimal64('0.499', 3), 0), toDecimalString(toDecimal64('-0.499', 3), 0);

SELECT 'the widest fractional part of every type';
SELECT toDecimalString(toDecimal32('0.999999999', 9), 8);
SELECT toDecimalString(toDecimal64('0.999999999999999999', 18), 17);
SELECT toDecimalString(toDecimal128('0.' || repeat('9', 38), 38), 37);
SELECT toDecimalString(toDecimal256('0.' || repeat('9', 76), 76), 75);

SELECT 'the boundary of rounding up';
SELECT toDecimalString(toDecimal64('0.45', 2), 1), toDecimalString(toDecimal64('0.4499999999999999', 16), 1);

SELECT 'a fixed length equal to or wider than the scale rounds nothing';
SELECT toDecimalString(toDecimal64('9.995', 3), 3), toDecimalString(toDecimal64('9.995', 3), 5);
SELECT toDecimalString(toDecimal256('-0.5', 2), 76);
