-- Test: mapRemove with map keys and removal keys of mixed signedness
SELECT mapRemove(map(toInt64(-1), 'a', toInt64(1), 'b'), toUInt64(1));
SELECT mapRemove(map(toUInt64(18446744073709551615), 'max', toUInt64(1), 'one'), -1);
SELECT mapRemove(map(toInt64(-1), 'minus_one', toInt64(1), 'one'), toUInt64(18446744073709551615));
SELECT mapRemove(map([toInt8(-1)], 'a', [toInt8(1)], 'b'), [toUInt64(1)]);
SELECT number, mapRemove(map(toInt64(number) - 1, 'a', toInt64(number), 'b'), toUInt64(1)) FROM numbers(3) ORDER BY number;
SELECT number, mapRemove(map(toInt64(-1), 'a', toInt64(1), 'b'), toUInt64(number)) FROM numbers(3) ORDER BY number;
