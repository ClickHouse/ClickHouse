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

SELECT
    'Date',
    formatReadableDecimalSize(number),
    formatReadableSize(number),
    formatReadableQuantity(number),
    formatReadableTimeDelta(number)
FROM (SELECT number::Date AS number FROM numbers(2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT
    'Date32',
    formatReadableDecimalSize(number),
    formatReadableSize(number),
    formatReadableQuantity(number),
    formatReadableTimeDelta(number)
FROM (SELECT number::Date32 AS number FROM numbers(2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT
    'DateTime',
    formatReadableDecimalSize(number),
    formatReadableSize(number),
    formatReadableQuantity(number),
    formatReadableTimeDelta(number)
FROM (SELECT number::DateTime AS number FROM numbers(2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT
    'DateTime64(3)',
    formatReadableDecimalSize(number),
    formatReadableSize(number),
    formatReadableQuantity(number),
    formatReadableTimeDelta(number)
FROM (SELECT number::DateTime64(3) AS number FROM numbers(2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }