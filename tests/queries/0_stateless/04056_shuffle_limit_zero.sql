SELECT count()
FROM
(
    SELECT number
    FROM numbers(10)
    SHUFFLE
    LIMIT 0
    SETTINGS allow_experimental_shuffle_query = 1, enable_analyzer = 1
);
