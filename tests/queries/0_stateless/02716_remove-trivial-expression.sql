EXPLAIN actions = 1, optimize = 1
SELECT h
FROM
    (
        SELECT sipHash64(number) AS h
        FROM numbers_mt(1000000000.)
        ORDER BY number ASC
    )
        LIMIT 10
