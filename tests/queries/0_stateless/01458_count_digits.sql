SELECT countDigits(toDecimal32(0, 0)), countDigits(toDecimal32(42, 0)), countDigits(toDecimal32(4.2, 1)),
       countDigits(toDecimal64(0, 0)), countDigits(toDecimal64(42, 0)), countDigits(toDecimal64(4.2, 2)),
       countDigits(toDecimal128(0, 0)), countDigits(toDecimal128(42, 0)), countDigits(toDecimal128(4.2, 3));

SELECT countDigits(materialize(toDecimal32(4.2, 1))),
       countDigits(materialize(toDecimal64(4.2, 2))),
       countDigits(materialize(toDecimal128(4.2, 3)));

SELECT countDigits(toDecimal32(1, 9)), countDigits(toDecimal32(-1, 9)),
       countDigits(toDecimal64(1, 18)), countDigits(toDecimal64(-1, 18)),
       countDigits(toDecimal128(1, 38)), countDigits(toDecimal128(-1, 38));
       
SELECT countDigits(toInt8(42)), countDigits(toInt8(-42)), countDigits(toUInt8(42)),
       countDigits(toInt16(42)), countDigits(toInt16(-42)), countDigits(toUInt16(42)),
       countDigits(toInt32(42)), countDigits(toInt32(-42)), countDigits(toUInt32(42)),
       countDigits(toInt64(42)), countDigits(toInt64(-42)), countDigits(toUInt64(42));

SELECT countDigits(toInt8(0)), countDigits(toInt8(0)),  countDigits(toUInt8(0)),
       countDigits(toInt16(0)), countDigits(toInt16(0)), countDigits(toUInt16(0)),
       countDigits(toInt32(0)), countDigits(toInt32(0)), countDigits(toUInt32(0)),
       countDigits(toInt64(0)), countDigits(toInt64(0)), countDigits(toUInt64(0));
       
SELECT countDigits(toInt8(127)), countDigits(toInt8(-128)),  countDigits(toUInt8(255)),
       countDigits(toInt16(32767)), countDigits(toInt16(-32768)), countDigits(toUInt16(65535)),
       countDigits(toInt32(2147483647)), countDigits(toInt32(-2147483648)), countDigits(toUInt32(4294967295)),
       countDigits(toInt64(9223372036854775807)), countDigits(toInt64(-9223372036854775808)), countDigits(toUInt64(18446744073709551615));

SELECT countDigits(toNullable(toDecimal32(4.2, 1))), countDigits(materialize(toNullable(toDecimal32(4.2, 2)))),
       countDigits(toNullable(toDecimal64(4.2, 3))), countDigits(materialize(toNullable(toDecimal64(4.2, 4)))),
       countDigits(toNullable(toDecimal128(4.2, 5))), countDigits(materialize(toNullable(toDecimal128(4.2, 6))));
