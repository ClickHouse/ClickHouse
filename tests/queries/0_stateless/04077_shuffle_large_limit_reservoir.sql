SELECT
    count(),
    uniqExact(number),
    min(number) >= 0,
    max(number) < 1000000
FROM
(
    SELECT number
    FROM numbers_mt(1000000)
    SHUFFLE
    LIMIT 100000
    SETTINGS allow_experimental_shuffle_query = 1, enable_analyzer = 1
);
