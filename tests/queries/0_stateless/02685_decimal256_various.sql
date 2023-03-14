SELECT 1.1::Decimal(60, 30);
SELECT round(1.1::Decimal(60, 30));
SELECT round(1.1::Decimal(60, 30), 1);
SELECT round(1.234567890123456789012345678901::Decimal(60, 30), 1);
SELECT round(1.234567890123456789012345678901::Decimal(60, 30), 30);
SELECT round(1.234567890123456789012345678901::Decimal(60, 30), 31);
SELECT round(1.234567890123456789012345678901::Decimal(60, 30), 20);

SELECT hex(1.234567890123456789012345678901::Decimal(60, 30));
SELECT bin(1.234567890123456789012345678901::Decimal(60, 30));
SELECT reinterpret(unhex(hex(1.234567890123456789012345678901::Decimal(60, 30))), 'Decimal(60, 30)');

SELECT arraySum([1.2::Decimal(60, 30), 3.45::Decimal(61, 29)]);
SELECT arraySum([1.2::Decimal(60, 30), 3.45::Decimal(3, 2)]);

SELECT arrayCompact([1.2::Decimal(60, 30) AS x, x, x, x, 3.45::Decimal(3, 2) AS y, y, x, x]);

SELECT 1.2::Decimal(2, 1) IN (1.2::Decimal(60, 30), 3.4::Decimal(60, 30));
SELECT 1.23::Decimal(3, 2) IN (1.2::Decimal(60, 30), 3.4::Decimal(60, 30));
SELECT 1.2::Decimal(60, 30) IN (1.2::Decimal(2, 1));

SELECT toTypeName([1.2::Decimal(60, 30), 3.45::Decimal(3, 2)]);
SELECT toTypeName(arraySum([1.2::Decimal(60, 30), 3.45::Decimal(3, 2)]));
