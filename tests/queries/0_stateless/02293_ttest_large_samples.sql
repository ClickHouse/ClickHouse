SELECT roundBankers(result.1, 16), roundBankers(result.2, 16) FROM (
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


SELECT roundBankers(result.1, 16), roundBankers(result.2, 16) FROM (
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
