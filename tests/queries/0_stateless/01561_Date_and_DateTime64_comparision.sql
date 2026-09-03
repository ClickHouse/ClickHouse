SELECT
    n,
    toTypeName(dt64) AS dt64_typename,

    '<',
    dt64 < d,
    toDate(dt64) < d,
    dt64 < toDateTime64(d, 1, 'UTC'),

    '<=',
    dt64 <= d,
    toDate(dt64) <= d,
    dt64 <= toDateTime64(d, 1, 'UTC'),

    '=',
    dt64 = d,
    toDate(dt64) = d,
    dt64 = toDateTime64(d, 1, 'UTC'),

    '>=',
    dt64 >= d,
    toDate(dt64) >= d,
    dt64 >= toDateTime64(d, 1, 'UTC'),

    '>',
    dt64 > d,
    toDate(dt64) > d,
    dt64 > toDateTime64(d, 1, 'UTC'),

    '!=',
    dt64 != d,
    toDate(dt64) != d,
    dt64 != toDateTime64(d, 1, 'UTC')
FROM
(
    WITH toDateTime('2019-09-16 19:20:11') as val
    SELECT
        number - 1 as n,
        toDateTime64(val, 1, 'UTC') AS dt64,
        toDate(val, 'UTC') - n as d
    FROM system.numbers
    LIMIT 3
)
