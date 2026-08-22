SELECT
    'Int128',
    formatReadableDecimalSize(number),
    formatReadableSize(number),
    formatReadableQuantity(number),
    formatReadableTimeDelta(number)
FROM (SELECT number::Int128 AS number FROM numbers(2));

SELECT
    'Int128',
    formatReadableDecimalSize(number),
    formatReadableSize(number),
    formatReadableQuantity(number),
    formatReadableTimeDelta(number)
FROM (SELECT number::UInt256 AS number FROM numbers(2));

SELECT
    'Decimal32',
    formatReadableDecimalSize(number),
    formatReadableSize(number),
    formatReadableQuantity(number),
    formatReadableTimeDelta(number)
FROM (SELECT number::Decimal32(2) AS number FROM numbers(2));

SELECT
    'Decimal256',
    formatReadableDecimalSize(number),
    formatReadableSize(number),
    formatReadableQuantity(number),
    formatReadableTimeDelta(number)
FROM (SELECT number::Decimal256(2) AS number FROM numbers(2));

SELECT
    'BFloat16',
    formatReadableDecimalSize(number),
    formatReadableSize(number),
    formatReadableQuantity(number),
    formatReadableTimeDelta(number)
FROM (SELECT number AS number FROM numbers(2));

SELECT formatReadableDecimalSize(number::Date) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableSize(number::Date) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableQuantity(number::Date) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableTimeDelta(number::Date) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT formatReadableDecimalSize(number::Date32) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableSize(number::Date32) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableQuantity(number::Date32) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableTimeDelta(number::Date32) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT formatReadableDecimalSize(number::DateTime) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableSize(number::DateTime) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableQuantity(number::DateTime) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableTimeDelta(number::DateTime) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT formatReadableDecimalSize(number::DateTime64(3)) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableSize(number::DateTime64(3)) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableQuantity(number::DateTime64(3)) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableTimeDelta(number::DateTime64(3)) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
