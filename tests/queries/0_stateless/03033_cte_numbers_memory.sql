-- https://github.com/ClickHouse/ClickHouse/issues/61238
SET enable_analyzer=1;

WITH
(SELECT number FROM system.numbers LIMIT 1) as w1,
(SELECT number FROM system.numbers LIMIT 1) as w2,
(SELECT number FROM system.numbers LIMIT 1) as w3,
(SELECT number FROM system.numbers LIMIT 1) as w4,
(SELECT number FROM system.numbers LIMIT 1) as w5,
(SELECT number FROM system.numbers LIMIT 1) as w6
SELECT number FROM (
    SELECT number FROM system.numbers LIMIT 10
    UNION ALL
    SELECT number FROM system.numbers LIMIT 10
)
WHERE number < 5;
