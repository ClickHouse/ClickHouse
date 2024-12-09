SELECT number as x, roundDown(x, [0, 1, 2, 3, 4, 5]) FROM system.numbers LIMIT 10;
SELECT toUInt8(number) as x, roundDown(x, [-1.5, e(), pi(), 5.5]) FROM system.numbers LIMIT 10;
SELECT toInt32(number) as x, roundDown(x, [e(), pi(), pi(), e()]) FROM system.numbers LIMIT 10;
SELECT number as x, roundDown(x, [6, 5, 4]) FROM system.numbers LIMIT 10;
SELECT 1 as x, roundDown(x, [6, 5, 4]);

SET send_logs_level = 'fatal';
SELECT 1 as x, roundDown(x, []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT 1 as x, roundDown(x, emptyArrayUInt8()); -- { serverError ILLEGAL_COLUMN }
SELECT roundDown(number, [number]) FROM system.numbers LIMIT 10; -- { serverError ILLEGAL_COLUMN }

SELECT 1 as x, roundDown(x, [1]);
SELECT 1 as x, roundDown(x, [1.5]);

SELECT number % 10 as x, roundDown(x, (SELECT groupArray(number * 1.25) FROM numbers(100000))) FROM system.numbers LIMIT 10;

SELECT toDecimal64(number, 5) / 100 as x, roundDown(x, [4, 5, 6]) FROM system.numbers LIMIT 10;
SELECT toDecimal64(number, 5) / 100 as x, roundDown(x, [toDecimal64(0.04, 5), toDecimal64(0.05, 5), toDecimal64(0.06, 5)]) FROM system.numbers LIMIT 10;
SELECT toDecimal64(number, 5) / 100 as x, roundDown(x, [toDecimal32(0.04, 2), toDecimal32(0.05, 2), toDecimal32(0.06, 2)]) FROM system.numbers LIMIT 10;
