SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 1; -- Changes query plan

CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=Memory;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

EXPLAIN actions = 1, keep_logical_steps = 1
SELECT *
FROM users u1 LEFT ANY JOIN
(
    SELECT sum(age)::Nullable(Int64) AS age_sum, name
    FROM users
    GROUP BY name
) u2
ON u1.name = u2.name
WHERE uid < age_sum;

SELECT *
FROM users u1 LEFT ANY JOIN
(
    SELECT sum(age)::Nullable(Int64) AS age_sum, name
    FROM users
    GROUP BY name
) u2
ON u1.name = u2.name
WHERE uid < age_sum;

-- Do not convert to INNER JOIN
EXPLAIN actions = 1, keep_logical_steps = 1
SELECT *
FROM users u1 LEFT ANY JOIN
(
    SELECT sum(age)::Nullable(Int64) AS age_sum, name
    FROM users
    GROUP BY name WITH ROLLUP
) u2
ON u1.name = u2.name
WHERE uid < age_sum;

-- Do not convert to INNER JOIN
EXPLAIN actions = 1, keep_logical_steps = 1
SELECT *
FROM users u1 LEFT ANY JOIN
(
    SELECT sum(age)::Nullable(Int64) AS age_sum, name
    FROM users
    GROUP BY name WITH CUBE
) u2
ON u1.name = u2.name
WHERE uid < age_sum;

-- Do not convert to INNER JOIN
EXPLAIN actions = 1, keep_logical_steps = 1
SELECT *
FROM users u1 LEFT ANY JOIN
(
    SELECT sum(age)::Nullable(Int64) AS age_sum, name
    FROM users
    GROUP BY GROUPING SETS ((name), ())
) u2
ON u1.name = u2.name
WHERE uid < age_sum;
