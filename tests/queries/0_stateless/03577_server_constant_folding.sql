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

-- Old analyzer has some issues with aliases for `remote()`, so the queries
-- executed w/o are slightly different, and required mostly to confirm the
-- same behavior for resolving shardNum() result value.

SELECT '-- JOIN subquery, analyzer';

SELECT shardNum(), tab.number
FROM remote('127.0.0.{1..3}', numbers(100)) tab
    ALL JOIN (
        SELECT number FROM numbers(10) WHERE number = shardNum()
    ) flt ON tab.number = flt.number
ORDER BY 1, 2
SETTINGS enable_analyzer = 1;

SELECT '-- JOIN subquery, w/o analyzer';

SELECT shardNum(), number
FROM remote('127.0.0.{1..3}', numbers(100))
    ALL JOIN (
        SELECT number AS flt_number FROM numbers(10) WHERE number = shardNum()
    ) flt ON number = flt_number
ORDER BY 1, 2
SETTINGS enable_analyzer = 0, joined_subquery_requires_alias = 0;

SELECT '-- GLOBAL JOIN subquery, analyzer';

SELECT shardNum(), tab.number
FROM remote('127.0.0.{1..3}', numbers(100)) tab
    GLOBAL ALL JOIN (
        SELECT number FROM numbers(10) WHERE number = shardNum()
    ) flt ON tab.number = flt.number
ORDER BY 1, 2
SETTINGS enable_analyzer = 1;

SELECT '-- GLOBAL JOIN subquery, w/o analyzer';

SELECT shardNum(), number
FROM remote('127.0.0.{1..3}', numbers(100))
    GLOBAL ALL JOIN (
        SELECT number AS flt_number FROM numbers(10) WHERE number = shardNum()
    ) flt ON number = flt_number
ORDER BY 1, 2
SETTINGS enable_analyzer = 0, joined_subquery_requires_alias = 0;

SELECT '-- JOIN union, analyzer';

SELECT shardNum(), tab.number
FROM remote('127.0.0.{1..3}', numbers(100)) tab
    ALL JOIN (
        SELECT number FROM numbers(10) WHERE number = shardNum()
        UNION ALL
        SELECT number FROM numbers(10) WHERE number = shardNum() * 2
    ) flt ON tab.number = flt.number
ORDER BY 1, 2
SETTINGS enable_analyzer = 1;

SELECT '-- JOIN union, w/o analyzer';

SELECT shardNum(), number
FROM remote('127.0.0.{1..3}', numbers(100))
    ALL JOIN (
        SELECT number AS flt_number FROM numbers(10) WHERE number = shardNum()
        UNION ALL
        SELECT number AS flt_number FROM numbers(10) WHERE number = shardNum() * 2
    ) flt ON number = flt_number
ORDER BY 1, 2
SETTINGS enable_analyzer = 0, joined_subquery_requires_alias = 0;

SELECT '-- GLOBAL JOIN union, analyzer';

SELECT shardNum(), tab.number
FROM remote('127.0.0.{1..3}', numbers(100)) tab
    GLOBAL ALL JOIN (
        SELECT number FROM numbers(10) WHERE number = shardNum()
        UNION ALL
        SELECT number FROM numbers(10) WHERE number = shardNum() * 2
    ) flt ON tab.number = flt.number
ORDER BY 1, 2
SETTINGS enable_analyzer = 1;

SELECT '-- GLOBAL JOIN union, w/o analyzer';

SELECT shardNum(), number
FROM remote('127.0.0.{1..3}', numbers(100))
    GLOBAL ALL JOIN (
        SELECT number AS flt_number FROM numbers(10) WHERE number = shardNum()
        UNION ALL
        SELECT number AS flt_number FROM numbers(10) WHERE number = shardNum() * 2
    ) flt ON number = flt_number
ORDER BY 1, 2
SETTINGS enable_analyzer = 0, joined_subquery_requires_alias = 0;
