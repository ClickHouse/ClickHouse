SELECT 'empty and rejected batches';
SELECT isNull(productIf(toFloat64(number), number > 100))
FROM numbers(12)
SETTINGS max_threads = 1, max_block_size = 3;
SELECT isNull(productIf(toFloat64(number), number > 100))
FROM numbers(0);

-- Reject whole batches before and after the selected rows. Nonzero UInt8 flags
-- must select rows even when they are not equal to one.
SELECT 'selection across batch boundaries';
SELECT productIf(
    multiIf(number = 4, 2., number = 8, 3., toFloat64('nan')),
    multiIf(number = 4, toUInt8(128), number = 8, toUInt8(255), toUInt8(0)))
FROM numbers(12)
SETTINGS max_threads = 1, max_block_size = 3;
SELECT productIf(
    multiIf(number = 4, 2., number = 8, 3., toFloat64('nan')),
    multiIf(number = 4, toUInt8(128), number = 8, toUInt8(255), toUInt8(0)))
FROM numbers(12)
SETTINGS max_threads = 1, max_block_size = 1;
SELECT productIf(
    multiIf(number = 4, 2., number = 8, 3., toFloat64('nan')),
    multiIf(number = 4, toUInt8(128), number = 8, toUInt8(255), toUInt8(0)))
FROM numbers(12)
SETTINGS max_threads = 1, max_block_size = 65536;

SELECT 'nullable selection across batch boundaries';
SELECT productIf(
    if(number % 4 = 0, NULL, multiIf(number = 5, 2., number = 9, 3., toFloat64('nan'))),
    if(number IN (0, 4, 5, 8, 9), toUInt8(255), toUInt8(0)))
FROM numbers(12)
SETTINGS max_threads = 1, max_block_size = 3;
SELECT product(multiIf(number = 6, 2., number = 7, 3., NULL))
FROM numbers(12)
SETTINGS max_threads = 1, max_block_size = 3;
SELECT isNull(productIf(if(number % 2 = 0, NULL, 2.), number % 2 = 0))
FROM numbers(12)
SETTINGS max_threads = 1, max_block_size = 3;

SELECT 'selected zero is not an empty state';
SELECT reinterpretAsUInt64(assumeNotNull(productIf(
    if(number = 4, toFloat64('-0'), toFloat64('nan')), number = 4)))
FROM numbers(12)
SETTINGS max_threads = 1, max_block_size = 3;

-- The sentinel bit pattern is also a valid NaN input. Once selected, it must
-- remain a NaN result rather than letting a later batch replace it with 2.
SELECT 'selected NaN is not an empty state';
SELECT isNull(productIf(
    if(number = 4, reinterpretAsFloat64(toUInt64(9218868437227405313)), 2.),
    number IN (4, 8)))
FROM numbers(12)
SETTINGS max_threads = 1, max_block_size = 3;

SELECT 'filtered decimal and wide integer batches';
SELECT productIf(toDecimal64(number + 1, 2), number IN (4, 8))
FROM numbers(12)
SETTINGS max_threads = 1, max_block_size = 3;
SELECT productIf(toInt256(number + 1), number IN (4, 8))
FROM numbers(12)
SETTINGS max_threads = 1, max_block_size = 3;
