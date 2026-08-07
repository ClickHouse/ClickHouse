SELECT cityHash64(toDecimal32(32, 2));
SELECT cityHash64(toDecimal64(64, 5));
SELECT cityHash64(toDecimal128(128, 24));
SELECT cityHash64(toDecimal32(number, 3)) from numbers(198, 2);
SELECT cityHash64(toDecimal64(number, 9)) from numbers(297, 2);
SELECT cityHash64(toDecimal128(number, 16)) from numbers(123, 2);
