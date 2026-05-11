SELECT 'precision = 0';
SELECT formatReadableSize(1024, 0);
SELECT formatReadableSize(1500, 0);
SELECT formatReadableSize(192851925, 0);

SELECT '--';
SELECT 'precision = 3';
SELECT formatReadableSize(1024, 3);
SELECT formatReadableSize(1500, 3);
SELECT formatReadableSize(192851925, 3);

SELECT '--';
SELECT 'precision = 7';
SELECT formatReadableSize(1024, 7);
SELECT formatReadableSize(1500, 7);
SELECT formatReadableSize(192851925, 7);

SELECT '--';
SELECT 'default precision (unchanged)';
SELECT formatReadableSize(1024);
SELECT formatReadableSize(192851925);

SELECT '--';
SELECT 'sibling functions accept precision';
SELECT formatReadableDecimalSize(1500, 0);
SELECT formatReadableDecimalSize(1500, 4);
SELECT formatReadableQuantity(1234567, 0);
SELECT formatReadableQuantity(1234567, 4);

SELECT '--';
SELECT 'Decimal input';
SELECT formatReadableSize(toDecimal64(192851925, 0), 4);
SELECT formatReadableDecimalSize(toDecimal64(192851925, 0), 4);
SELECT formatReadableQuantity(toDecimal64(1234567, 0), 4);

SELECT '--';
SELECT 'formatReadableSize: full value range, mixed input types, precision = 3';
WITH round(exp(number), 6) AS x,
     x > 0xFFFFFFFFFFFFFFFF ? 0xFFFFFFFFFFFFFFFF : toUInt64(x) AS y,
     x > 0x7FFFFFFF ? 0x7FFFFFFF : toInt32(x) AS z
SELECT formatReadableSize(x, 3), formatReadableSize(y, 3), formatReadableSize(z, 3)
FROM system.numbers
LIMIT 70;

SELECT '--';
SELECT 'formatReadableDecimalSize: full value range, mixed input types, precision = 3';
WITH round(exp(number), 6) AS x,
     x > 0xFFFFFFFFFFFFFFFF ? 0xFFFFFFFFFFFFFFFF : toUInt64(x) AS y,
     x > 0x7FFFFFFF ? 0x7FFFFFFF : toInt32(x) AS z
SELECT formatReadableDecimalSize(x, 3), formatReadableDecimalSize(y, 3), formatReadableDecimalSize(z, 3)
FROM system.numbers
LIMIT 70;

SELECT '--';
SELECT 'formatReadableQuantity: full value range, mixed input types, precision = 3';
WITH round(exp(number), 6) AS x,
     x > 0xFFFFFFFFFFFFFFFF ? 0xFFFFFFFFFFFFFFFF : toUInt64(x) AS y,
     x > 0x7FFFFFFF ? 0x7FFFFFFF : toInt32(x) AS z
SELECT formatReadableQuantity(x, 3), formatReadableQuantity(y, 3), formatReadableQuantity(z, 3)
FROM system.numbers
LIMIT 70;

SELECT '--';
SELECT 'special floating-point values';
SELECT formatReadableSize(nan, 3);
SELECT formatReadableSize(inf, 3);
SELECT formatReadableSize(-inf, 3);
SELECT formatReadableDecimalSize(nan, 3);
SELECT formatReadableQuantity(inf, 3);

SELECT '--';
SELECT 'NULL propagation';
SELECT formatReadableSize(toNullable(1024), 3);
SELECT formatReadableSize(CAST(NULL AS Nullable(UInt64)), 3);

SELECT '--';
SELECT 'errors: precision argument shape';
SELECT formatReadableSize(1024, 'two'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableSize(1024, 1.5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableSize(1024, -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableSize(1024, 256); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableSize(1024, toUInt8(number)) FROM numbers(2); -- { serverError ILLEGAL_COLUMN }

SELECT '--';
SELECT 'errors: argument count';
SELECT formatReadableSize(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT formatReadableSize(1024, 2, 3); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
