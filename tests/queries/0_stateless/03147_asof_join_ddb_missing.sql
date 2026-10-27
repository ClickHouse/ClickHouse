SET enable_analyzer=1;

SET session_timezone = 'UTC';
SET joined_subquery_requires_alias = 0;
SET enable_analyzer = 1;
SET join_algorithm = 'full_sorting_merge';

-- # 10 dates, 5 keys
WITH build AS (
    SELECT
        k,
        toDateTime('2001-01-01 00:00:00') + INTERVAL number MINUTE AS t,
        number AS v
    FROM numbers(10), (SELECT number AS k FROM numbers(5))
    SETTINGS join_algorithm = 'default'
),
probe AS (
    SELECT
        k * 2 AS k,
        t - INTERVAL 30 SECOND AS t
    FROM build
)
SELECT SUM(v)
FROM probe ASOF JOIN build USING (k, t);

-- # Coverage: Missing right side bin
WITH build AS (
    SELECT
        k * 2 AS k,
        toDateTime('2001-01-01 00:00:00') + INTERVAL number MINUTE AS t,
        number AS v
    FROM numbers(10), (SELECT number AS k FROM numbers(5))
    SETTINGS join_algorithm = 'default'
),
probe AS (
    SELECT
        intDiv(k, 2) AS k,
        t - INTERVAL 30 SECOND AS t
    FROM build
)
SELECT SUM(v), COUNT(*)
FROM probe ASOF JOIN build USING (k, t);

-- # 20 dates, 5 keys
WITH build AS (
    SELECT
        k,
        toDateTime('2001-01-01 00:00:00') + INTERVAL number MINUTE AS t,
        number AS v
    FROM numbers(20), (SELECT number AS k FROM numbers(5))
    SETTINGS join_algorithm = 'default'
),
probe AS (
    SELECT
        k * 2 AS k,
        t - INTERVAL 30 SECOND AS t
    FROM build
)
SELECT SUM(v)
FROM probe ASOF JOIN build USING (k, t);

-- # 30 dates, 5 keys
WITH build AS (
    SELECT
        k,
        toDateTime('2001-01-01 00:00:00') + INTERVAL number MINUTE AS t,
        number AS v
    FROM numbers(30), (SELECT number AS k FROM numbers(5))
    SETTINGS join_algorithm = 'default'
),
probe AS (
    SELECT
        k * 2 AS k,
        t - INTERVAL 30 SECOND AS t
    FROM build
)
SELECT SUM(v)
FROM probe ASOF JOIN build USING (k, t);

-- # 50 dates, 5 keys
WITH build AS (
    SELECT
        k,
        toDateTime('2001-01-01 00:00:00') + INTERVAL number MINUTE AS t,
        number AS v
    FROM numbers(50), (SELECT number AS k FROM numbers(5))
    SETTINGS join_algorithm = 'default'
),
probe AS (
    SELECT
        k * 2 AS k,
        t - INTERVAL 30 SECOND AS t
    FROM build
)
SELECT SUM(v)
FROM probe ASOF JOIN build USING (k, t);

-- # 100 dates, 5 keys
WITH build AS (
    SELECT
        k,
        toDateTime('2001-01-01 00:00:00') + INTERVAL number MINUTE AS t,
        number AS v
    FROM numbers(100), (SELECT number AS k FROM numbers(5))
    SETTINGS join_algorithm = 'default'
),
probe AS (
    SELECT
        k * 2 AS k,
        t - INTERVAL 30 SECOND AS t
    FROM build
)
SELECT SUM(v)
FROM probe ASOF JOIN build USING (k, t);

-- # 100 dates, 50 keys
WITH build AS (
    SELECT
        k,
        toDateTime('2001-01-01 00:00:00') + INTERVAL number MINUTE AS t,
        number AS v
    FROM numbers(100), (SELECT number AS k FROM numbers(50))
    SETTINGS join_algorithm = 'default'
),
probe AS (
    SELECT
        k * 2 AS k,
        t - INTERVAL 30 SECOND AS t
    FROM build
)
SELECT SUM(v)
FROM probe ASOF JOIN build USING (k, t);

-- # 1000 dates, 5 keys
WITH build AS (
    SELECT
        k,
        toDateTime('2001-01-01 00:00:00') + INTERVAL number MINUTE AS t,
        number AS v
    FROM numbers(1000), (SELECT number AS k FROM numbers(5))
    SETTINGS join_algorithm = 'default'
),
probe AS (
    SELECT
        k * 2 AS k,
        t - INTERVAL 30 SECOND AS t
    FROM build
)
SELECT SUM(v)
FROM probe ASOF JOIN build USING (k, t);

-- # 1000 dates, 50 keys
WITH build AS (
    SELECT
        k,
        toDateTime('2001-01-01 00:00:00') + INTERVAL number MINUTE AS t,
        number AS v
    FROM numbers(1000), (SELECT number AS k FROM numbers(50))
    SETTINGS join_algorithm = 'default'
),
probe AS (
    SELECT
        k * 2 AS k,
        t - INTERVAL 30 SECOND AS t
    FROM build
)
SELECT SUM(v)
FROM probe ASOF JOIN build USING (k, t);

-- # 10000 dates, 50 keys
WITH build AS (
    SELECT
        k,
        toDateTime('2001-01-01 00:00:00') + INTERVAL number MINUTE AS t,
        number AS v
    FROM numbers(10000), (SELECT number AS k FROM numbers(50))
    SETTINGS join_algorithm = 'default'
),
probe AS (
    SELECT
        k * 2 AS k,
        t - INTERVAL 30 SECOND AS t
    FROM build
)
SELECT SUM(v)
FROM probe ASOF JOIN build USING (k, t);
