EXPLAIN SYNTAX
SELECT msg, toDateTime(intDiv(ms, 1000)) AS time
FROM
(
    SELECT
        'hello' AS msg,
        toUInt64(t) * 1000 AS ms
    FROM generateRandom('t DateTime')
    LIMIT 10
)
ORDER BY msg, time;
