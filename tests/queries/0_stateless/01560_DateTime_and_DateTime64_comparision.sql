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
    SELECT
        number - 1 as n,
        toDateTime64(toStartOfInterval(now(), toIntervalSecond(1), 'UTC'), 1, 'UTC') + n AS dt64,
        toStartOfInterval(now(), toIntervalSecond(1), 'UTC') AS dt
    FROM system.numbers
    LIMIT 3
)
