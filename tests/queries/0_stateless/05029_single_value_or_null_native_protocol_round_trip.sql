SELECT id, singleValueOrNullMerge(state)
FROM remote('127.0.0.2', view(
    SELECT 1 AS id, singleValueOrNullState(toUInt64(42)) AS state
    UNION ALL
    SELECT 2 AS id, singleValueOrNullState(number) AS state
    FROM numbers(2)
))
GROUP BY id
ORDER BY id;
