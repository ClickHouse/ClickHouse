SELECT countDigits(0);
SELECT countDigits(1);
SELECT countDigits(-1);
SELECT countDigits(12345);
SELECT countDigits(-12345);
SELECT countDigits(0xFFFFFFFFFFFFFFFF);
SELECT countDigits(CAST(0x8000000000000000 AS Int64));
SELECT countDigits(CAST(-1 AS UInt128));
SELECT countDigits(CAST(-1 AS UInt256));
SELECT countDigits(CAST(CAST(-1 AS UInt128) DIV 2 + 1 AS Int128));
SELECT countDigits(CAST(CAST(-1 AS UInt256) DIV 2 + 1 AS Int256));

SELECT countDigits(-123.45678::Decimal32(5));
SELECT countDigits(-123.456789::Decimal64(6));
SELECT countDigits(-123.4567890::Decimal128(7));
SELECT countDigits(-123.45678901::Decimal256(8));

-- this behavior can be surprising, but actually reasonable:
SELECT countDigits(-123.456::Decimal32(5));
SELECT countDigits(-123.4567::Decimal64(6));
SELECT countDigits(-123.45678::Decimal128(7));
SELECT countDigits(-123.456789::Decimal256(8));
