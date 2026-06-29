SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

CREATE TABLE users (`uid` Int16, `name` String, `age` Int16) ENGINE = Memory;

-- Analyze materialized CTE subquery after join tree in the FROM clause is initiated.
-- This allows to avoid LOGICAL_ERROR during 'users__fuzz_29' identifier resolution
-- when indentifier resolution in parent scope is performed.
WITH
    a AS MATERIALIZED (
        SELECT DISTINCT
            uid
        FROM
            users
        GROUP BY
            1,
            toUInt8 (users__fuzz_29, 5),
            1
        WITH
            CUBE
        LIMIT
            2
    )
SELECT
    count(),
    toUInt256 (1023) <=> 5
FROM
    a
WHERE
    uid IN (
        SELECT
            uid
        FROM
            a
    ); -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS a;
