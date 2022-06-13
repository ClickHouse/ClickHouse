EXPLAIN PIPELINE
SELECT * FROM
(
    SELECT * FROM system.numbers LIMIT 10
) t1
ALL LEFT JOIN
(
    SELECT * FROM system.numbers LIMIT 10
) t2
USING number
SETTINGS max_threads=16;
