-- An integer that does not fit the element type of a quoted composite literal is rejected instead of
-- wrapping to an unrelated value, at any size of the literal.

SELECT [toUInt64(0)] = '[18446744073709551616]';
SELECT [toUInt8(44)] = '[300]';
SELECT [toInt8(127)] = '[-129]';
SELECT (toUInt8(44), toUInt8(1)) = '(300,1)';
SELECT map('a', toUInt64(0)) = '{''a'':18446744073709551616}';
SELECT [[toUInt64(0)]] = '[[18446744073709551616]]';
SELECT [toUInt8(44)] = '[115792089237316195423570985008687907853269984665640564039457584007913129639980]';

SELECT * FROM values('x Array(UInt64)', '[18446744073709551616]'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('x Array(UInt8)', '[300]'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('x Tuple(a UInt64, b UInt64)', '(18446744073709551616,1)'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('x Map(UInt8, UInt8)', '{300:1}'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('x Array(Nullable(UInt64))', '[18446744073709551616]'); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- one past the maximum of the widest unsigned type, and one past the minimum of the widest signed one
SELECT * FROM values('x Array(UInt256)', '[115792089237316195423570985008687907853269984665640564039457584007913129639936]'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('x Array(Int256)', '[-57896044618658097711785492504343953926634992332820282019728792003956564819969]'); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- the exact bounds of every width are still accepted: these are the lines a too-eager threshold test
-- would break, and each one already passes today
SELECT * FROM values('x Array(UInt256)', '[115792089237316195423570985008687907853269984665640564039457584007913129639935]');
SELECT * FROM values('x Array(Int256)', '[-57896044618658097711785492504343953926634992332820282019728792003956564819968]');
SELECT * FROM values('x Array(Int8)', '[-128,127]');
SELECT * FROM values('x Array(UInt64)', '[0,1,18446744073709551615]');

-- a nested value is rejected at any depth, and a wide element of a column that stores one of several
-- types is rejected exactly as a narrow one already is
SELECT * FROM values('x Array(Array(UInt64))', '[[18446744073709551616]]'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SET allow_experimental_variant_type = 1, allow_suspicious_variant_types = 1;
SELECT CAST([toUInt128(0)], 'Array(Variant(String, UInt128))') = '[340282366920938463463374607431768211456]'; -- { serverError INCORRECT_DATA }
SELECT CAST([toUInt8(44)], 'Array(Variant(String, UInt8))') = '[300]'; -- { serverError INCORRECT_DATA }
SELECT CAST([toUInt128(7)], 'Array(Variant(String, UInt128))') = '[7]';

-- in-range values, and element types that read their own text, are unaffected
SELECT [toUInt64(18446744073709551615)] = '[18446744073709551615]';
SELECT [toUInt8(255)] = '[255]';
SELECT * FROM values('x Array(UInt64)', '[]');
SELECT * FROM values('x Array(Bool)', '[1]');
SELECT * FROM values($$x Array(Enum8('a' = 1))$$, $$['a']$$);
SELECT * FROM values('x Array(Decimal32(2))', '[1.25]');
SELECT * FROM values('x Array(DateTime)', $$['2020-01-01 00:00:00']$$);
