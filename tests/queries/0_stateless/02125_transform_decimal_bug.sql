SELECT transform(1, [1], [toDecimal32(1, 2)]);
SELECT transform(toDecimal32(number, 2), [toDecimal32(3, 2)], [toDecimal32(30, 2)]) FROM system.numbers LIMIT 10;
SELECT transform(toDecimal32(number, 2), [toDecimal32(3, 2)], [toDecimal32(30, 2)], toDecimal32(1000, 2)) FROM system.numbers LIMIT 10;
SELECT transform(number, [3, 5, 11], [toDecimal32(30, 2), toDecimal32(50, 2), toDecimal32(70,2)], toDecimal32(1000, 2)) FROM system.numbers LIMIT 10;
SELECT transform(number, [3, 5, 11], [toDecimal32(30, 2), toDecimal32(50, 2), toDecimal32(70,2)], toDecimal32(1000, 2)) FROM system.numbers LIMIT 10;
SELECT transform(toString(number), ['3', '5', '7'], [toDecimal32(30, 2), toDecimal32(50, 2), toDecimal32(70,2)], toDecimal32(1000, 2)) FROM system.numbers LIMIT 10;





