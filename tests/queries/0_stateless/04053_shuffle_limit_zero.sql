SELECT count()
FROM
(
    SELECT number
    FROM numbers(10)
    WHERE 1
    SHUFFLE
    LIMIT 0
    SETTINGS allow_experimental_shuffle_query = 1
);
