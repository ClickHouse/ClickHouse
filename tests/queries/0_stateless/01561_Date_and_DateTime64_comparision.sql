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
    SELECT
        number - 1 as n,
        toDateTime64(toStartOfInterval(now(), toIntervalSecond(1), 'UTC'), 1, 'UTC') AS dt64,
        toDate(now(), 'UTC') - n as d
    FROM system.numbers
    LIMIT 3
)
FORMAT TabSeparated
