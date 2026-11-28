-- An integer element of a quoted composite literal that does not fit its type is rejected instead of
-- wrapping to an unrelated value, at any size of the literal.

SET allow_suspicious_low_cardinality_types = 1;

SELECT [toUInt64(0)] = '[18446744073709551616]';
SELECT [toUInt8(44)] = '[300]';
SELECT [toInt8(127)] = '[-129]';
SELECT [toInt8(-128)] = '[128]';
SELECT (toUInt8(44), toUInt8(1)) = '(300,1)';
SELECT map('a', toUInt64(0)) = '{''a'':18446744073709551616}';
SELECT [[toUInt64(0)]] = '[[18446744073709551616]]';
SELECT [toUInt8(44)] = '[115792089237316195423570985008687907853269984665640564039457584007913129639980]';

-- a comparison answers by value, like the same literal unquoted
SELECT [toUInt8(44)] != '[300]';
SELECT (toUInt8(44), toNullable(toUInt8(1))) = '(300,1)';
SELECT x != '(300,1)' FROM (SELECT (toUInt8(number + 44), toNullable(toUInt8(1))) AS x FROM numbers(1));
SELECT [toUInt8(43)] < '[300]', [toUInt8(43)] < [300];
SELECT (toUInt8(43), toUInt8(1)) >= '(300,1)', (toUInt8(43), toUInt8(1)) >= (300, 1);
SELECT '[300]' > [toUInt8(43)];
SELECT map(toUInt8(1), toUInt8(1)) <= '{300:1}';
SELECT CAST((toUInt8(43), toUInt8(1)) AS Nullable(Tuple(UInt8, UInt8))) >= '(300,1)';
SELECT [toUInt8(1), toUInt8(5)] < '[1,300]', [toUInt8(2), toUInt8(5)] < '[1,300]';
SELECT [toUInt8(0)] > '[-1]', [toUInt8(0)] = '[-1]';
SELECT map(toUInt8(0), toUInt256(0)) > '{-1:0}', map(toUInt8(0), toUInt256(0)) > map(-1, 0);
SELECT map(toUInt8(0), toUInt256(0)) = '{-1:0}', map(toUInt8(0), toUInt256(0)) = map(-1, 0);
SELECT [(toUInt256(0), toNullable(toUInt8(1)))] = '[(-1,1)]', [(toUInt256(0), toNullable(toUInt8(1)))] = [(-1,1)];

-- a pair of types that cannot be ordered, quoted or not
SELECT [(toUInt256(0), toNullable(toUInt8(1)))] < '[(-1,1)]'; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT [(toUInt256(0), toNullable(toUInt8(1)))] < [(-1,1)]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- a 256-bit element of either signedness holds the integer
SELECT [toUInt8(0)] < '[57896044618658097711785492504343953926634992332820282019728792003956564819968]', [toUInt256(0)] > '[-1]';
SELECT [toInt256(0)] < '[57896044618658097711785492504343953926634992332820282019728792003956564819968]', (toUInt8(0), toInt8(0)) < '(1,57896044618658097711785492504343953926634992332820282019728792003956564819968)';

-- an integer that no 256-bit integer element holds: an equality still answers, an ordering is refused
SELECT [toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968')] = '[-57896044618658097711785492504343953926634992332820282019728792003956564819969]', [toUInt256(0)] != '[115792089237316195423570985008687907853269984665640564039457584007913129639936]';
SELECT [toUInt8(0)] < '[115792089237316195423570985008687907853269984665640564039457584007913129639980]'; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT [toUInt8(0)] = '[115792089237316195423570985008687907853269984665640564039457584007913129639980x]'; -- { serverError CANNOT_READ_ARRAY_FROM_TEXT }
SELECT CAST((toUInt8(43), toUInt8(1)) AS Nullable(Tuple(UInt8, UInt8))) >= '(1,-57896044618658097711785492504343953926634992332820282019728792003956564819969)'; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- an out-of-range element of another type is still reported, also after an integer that does not fit
SELECT [toDateTime('2020-01-01 00:00:00', 'UTC')] = '[''2200-01-01 00:00:00'']' SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT (toUInt8(1), toDateTime('2020-01-01 00:00:00', 'UTC')) = '(300,''2200-01-01 00:00:00'')' SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT * FROM values('x Array(UInt64)', '[18446744073709551616]'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('x Array(UInt8)', '[300]'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('x Array(UInt8)', '[-1]'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('x Array(UInt8)', '[-0]'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('x Tuple(a UInt64, b UInt64)', '(18446744073709551616,1)'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('x Map(UInt8, UInt8)', '{300:1}'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('x Array(Nullable(UInt64))', '[18446744073709551616]'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('x Array(LowCardinality(UInt8))', '[300]'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('x Array(LowCardinality(Nullable(UInt8)))', '[300]'); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- one past the maximum of the widest unsigned type, and one past the minimum of the widest signed one
SELECT * FROM values('x Array(UInt256)', '[115792089237316195423570985008687907853269984665640564039457584007913129639936]'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('x Array(Int256)', '[-57896044618658097711785492504343953926634992332820282019728792003956564819969]'); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- the exact bounds of the widest types, and of a narrow one, are accepted
SELECT * FROM values('x Array(UInt256)', '[115792089237316195423570985008687907853269984665640564039457584007913129639935]');
SELECT * FROM values('x Array(Int256)', '[-57896044618658097711785492504343953926634992332820282019728792003956564819968]');
SELECT * FROM values('x Array(Int8)', '[-128,127]');
SELECT * FROM values('x Array(UInt64)', '[0,1,18446744073709551615]');

-- a nested value is rejected at any depth, and a wide Variant alternative is rejected like a narrow one
SELECT * FROM values('x Array(Array(UInt64))', '[[18446744073709551616]]'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST([toUInt128(0)], 'Array(Variant(String, UInt128))') = '[340282366920938463463374607431768211456]'; -- { serverError INCORRECT_DATA }
SELECT CAST([toUInt8(44)], 'Array(Variant(String, UInt8))') = '[300]'; -- { serverError INCORRECT_DATA }
SELECT CAST([toUInt128(7)], 'Array(Variant(String, UInt128))') = '[7]';

-- a composite alternative of a Variant column that no alternative holds is rejected
SELECT * FROM values('x Variant(Array(UInt128), UInt8)', '[340282366920938463463374607431768211456]'); -- { serverError INCORRECT_DATA }
SELECT * FROM values('x Variant(Tuple(UInt128), UInt8)', '(340282366920938463463374607431768211456)'); -- { serverError INCORRECT_DATA }

-- in-range values, and element types that read their own text, are unaffected
SELECT [toUInt64(18446744073709551615)] = '[18446744073709551615]';
SELECT [toUInt8(255)] = '[255]';
SELECT * FROM values('x Array(UInt64)', '[]');
SELECT * FROM values('x Array(Bool)', '[1]');
SELECT * FROM values($$x Array(Enum8('a' = 1))$$, $$['a']$$);
SELECT * FROM values('x Array(Decimal32(2))', '[1.25]');
SELECT * FROM values('x Array(DateTime)', $$['2020-01-01 00:00:00']$$);
