SELECT cityHash64(toDecimal32(32, 2)) = cityHash64(toString(toDecimal32(32, 2)));
SELECT cityHash64(toDecimal64(64, 5)) = cityHash64(toString(toDecimal64(64, 5)));
SELECT cityHash64(toDecimal128(128, 24)) = cityHash64(toString(toDecimal128(128, 24)));
SELECT count() FROM numbers(200, 2) WHERE cityHash64(toDecimal32(number, 4)) = cityHash64(toString(toDecimal32(number, 4)));
SELECT count() FROM numbers(2301, 2) WHERE cityHash64(toDecimal64(number, 6)) = cityHash64(toString(toDecimal64(number, 6)));
SELECT count() FROM numbers(2143, 2) WHERE cityHash64(toDecimal128(number, 10)) = cityHash64(toString(toDecimal128(number, 10)));

