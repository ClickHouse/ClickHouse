-- Tags: long

SELECT roundBankers(result.1, 5), roundBankers(result.2, 5) FROM (
SELECT
     studentTTest(sample, variant) as result
FROM (
SELECT
    toFloat64(number) % 30 AS sample,
    0 AS variant
FROM system.numbers limit 500000

UNION ALL

SELECT
    toFloat64(number) % 30 + 0.0022 AS sample,
    1 AS variant
FROM system.numbers limit 500000));


SET max_rows_to_read = 0;

SELECT roundBankers(result.1, 5), roundBankers(result.2, 5 ) FROM (
SELECT
     studentTTest(sample, variant) as result
FROM (
SELECT
    toFloat64(number) % 30 AS sample,
    0 AS variant
FROM system.numbers limit 50000000

UNION ALL

SELECT
    toFloat64(number) % 30 + 0.0022 AS sample,
    1 AS variant
FROM system.numbers limit 50000000));


SELECT roundBankers(result.2, 1025)
FROM
(
    SELECT studentTTest(sample, variant) AS result
    FROM
    (
        SELECT
            toFloat64(number) % 30 AS sample,
            1048576 AS variant
        FROM system.numbers
        LIMIT 1
        UNION ALL
        SELECT
            (toFloat64(number) % 7) + inf AS sample,
            255 AS variant
        FROM system.numbers
        LIMIT 1023
    )
);
