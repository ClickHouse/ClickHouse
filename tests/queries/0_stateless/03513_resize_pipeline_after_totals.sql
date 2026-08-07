EXPLAIN PIPELINE
SELECT cityHash64(number)
FROM numbers_mt(100)
GROUP BY number
    WITH TOTALS
SETTINGS max_threads = 4;
