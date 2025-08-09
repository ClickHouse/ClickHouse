SET prefer_localhost_replica = 0;

SELECT '-- IN subquery';

SELECT shardNum(), number
FROM remote('127.0.0.{1..3}', numbers(100))
WHERE number IN (
    SELECT number FROM numbers(10) WHERE number = shardNum()
)
ORDER BY 1, 2;

SELECT '-- GLOBAL IN subquery';

SELECT shardNum(), number
FROM remote('127.0.0.{1..3}', numbers(100))
WHERE number GLOBAL IN (
    SELECT number FROM numbers(10) WHERE number = shardNum()
)
ORDER BY 1, 2;

SELECT '-- IN union';

SELECT shardNum(), number
FROM remote('127.0.0.{1..3}', numbers(100))
WHERE number IN (
    SELECT number FROM numbers(10) WHERE number = shardNum()
    UNION ALL
    SELECT number FROM numbers(10) WHERE number = shardNum() * 2
)
ORDER BY 1, 2;

SELECT '-- GLOBAL IN union';

SELECT shardNum(), number
FROM remote('127.0.0.{1..3}', numbers(100))
WHERE number GLOBAL IN (
    SELECT number FROM numbers(10) WHERE number = shardNum()
    UNION ALL
    SELECT number FROM numbers(10) WHERE number = shardNum() * 2
)
ORDER BY 1, 2;

SELECT '-- IN CTE subquery';

WITH flt AS (
    SELECT number FROM numbers(10) WHERE number = shardNum()
)
SELECT shardNum(), number
FROM remote('127.0.0.{1..3}', numbers(100))
WHERE number IN (flt)
ORDER BY 1, 2;

SELECT '-- GLOBAL IN CTE subquery';

WITH flt AS (
    SELECT number FROM numbers(10) WHERE number = shardNum()
)
SELECT shardNum(), number
FROM remote('127.0.0.{1..3}', numbers(100))
WHERE number GLOBAL IN (flt)
ORDER BY 1, 2;

SELECT '-- IN CTE union';

WITH flt AS (
    SELECT number FROM numbers(10) WHERE number = shardNum()
    UNION ALL
    SELECT number FROM numbers(10) WHERE number = shardNum() * 2
)
SELECT shardNum(), number
FROM remote('127.0.0.{1..3}', numbers(100))
WHERE number IN (flt)
ORDER BY 1, 2;

SELECT '-- GLOBAL IN CTE union';

WITH flt AS (
    SELECT number FROM numbers(10) WHERE number = shardNum()
    UNION ALL
    SELECT number FROM numbers(10) WHERE number = shardNum() * 2
)
SELECT shardNum(), number
FROM remote('127.0.0.{1..3}', numbers(100))
WHERE number GLOBAL IN (flt)
ORDER BY 1, 2;

SELECT '-- JOIN subquery';

SELECT shardNum(), tab.number
FROM remote('127.0.0.{1..3}', numbers(100)) tab
    ALL JOIN (
        SELECT number FROM numbers(10) WHERE number = shardNum()
    ) flt ON tab.number = flt.number
ORDER BY 1, 2;

SELECT '-- GLOBAL JOIN subquery';

SELECT shardNum(), tab.number
FROM remote('127.0.0.{1..3}', numbers(100)) tab
    GLOBAL ALL JOIN (
        SELECT number FROM numbers(10) WHERE number = shardNum()
    ) flt ON tab.number = flt.number
ORDER BY 1, 2;

SELECT '-- JOIN union';

SELECT shardNum(), tab.number
FROM remote('127.0.0.{1..3}', numbers(100)) tab
    ALL JOIN (
        SELECT number FROM numbers(10) WHERE number = shardNum()
        UNION ALL
        SELECT number FROM numbers(10) WHERE number = shardNum() * 2
    ) flt ON tab.number = flt.number
ORDER BY 1, 2;

SELECT '-- GLOBAL JOIN union';

SELECT shardNum(), tab.number
FROM remote('127.0.0.{1..3}', numbers(100)) tab
    GLOBAL ALL JOIN (
        SELECT number FROM numbers(10) WHERE number = shardNum()
        UNION ALL
        SELECT number FROM numbers(10) WHERE number = shardNum() * 2
    ) flt ON tab.number = flt.number
ORDER BY 1, 2;
