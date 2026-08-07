SELECT
    n,
    toTypeName(dt64) AS dt64_typename,

    '<',
    dt64 < dt,
    toDateTime(dt64) < dt,
    dt64 < toDateTime64(dt, 1, 'UTC'),
    
    '<=',
    dt64 <= dt,
    toDateTime(dt64) <= dt,
    dt64 <= toDateTime64(dt, 1, 'UTC'),

    '=',
    dt64 = dt,
    toDateTime(dt64) = dt,
    dt64 = toDateTime64(dt, 1, 'UTC'),
    
    '>=',
    dt64 >= dt,
    toDateTime(dt64) >= dt,
    dt64 >= toDateTime64(dt, 1, 'UTC'),
    
    '>',
    dt64 > dt,
    toDateTime(dt64) > dt,
    dt64 > toDateTime64(dt, 1, 'UTC'),

    '!=',
    dt64 != dt,
    toDateTime(dt64) != dt,
    dt64 != toDateTime64(dt, 1, 'UTC')
FROM
(
    WITH toDateTime('2015-05-18 07:40:11') as value
    SELECT
        number - 1 as n,
        toDateTime64(value, 1, 'UTC') AS dt64,
        value - n as dt
    FROM system.numbers
    LIMIT 3
)
