SELECT toDecimal32OrDefault(111, 3, 123.456::Decimal32(3)) AS x, toTypeName(x);
SELECT toDecimal64OrDefault(222, 3, 123.456::Decimal64(3)) AS x, toTypeName(x);
SELECT toDecimal128OrDefault(333, 3, 123.456::Decimal128(3)) AS x, toTypeName(x);
SELECT toDecimal256OrDefault(444, 3, 123.456::Decimal256(3)) AS x, toTypeName(x);

SELECT toDecimal32OrDefault('Hello', 3, 123.456::Decimal32(3)) AS x, toTypeName(x);
SELECT toDecimal64OrDefault('Hello', 3, 123.456::Decimal64(3)) AS x, toTypeName(x);
SELECT toDecimal128OrDefault('Hello', 3, 123.456::Decimal128(3)) AS x, toTypeName(x);
SELECT toDecimal256OrDefault('Hello', 3, 123.456::Decimal256(3)) AS x, toTypeName(x);
