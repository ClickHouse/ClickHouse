SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 1; -- Changes query plan
SET enable_join_runtime_filters = 0;

CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=Memory;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

-- reverse is an injective function, thus it can be converted to INNER JOIN
EXPLAIN actions = 1, keep_logical_steps = 1
SELECT *
FROM users u1 LEFT ANY JOIN
(
    SELECT sum(age)::Nullable(Int64) AS age_sum, name
    FROM users
    GROUP BY name
) u2
ON u1.name = reverse(u2.name)
WHERE uid < age_sum;

-- upper is not an injective function, thus it cannot be converted to INNER JOIN
EXPLAIN actions = 1, keep_logical_steps = 1
SELECT *
FROM users u1 LEFT ANY JOIN
(
    SELECT sum(age)::Nullable(Int64) AS age_sum, name
    FROM users
    GROUP BY name
) u2
ON u1.name = upper(u2.name)
WHERE uid < age_sum;

-- The only difference of the following two queries is the second condition in the JOIN clause.
-- The aim for them is to verify that the presence of a non equivalence condition prevents the
-- conversion to INNER JOIN, even if the columns used by the non equivalence condition are used
-- as grouping keys. Uniqueness of matching rows cannot be guaranteed in this case.
EXPLAIN actions = 1, keep_logical_steps = 1
SELECT *
FROM users u1 LEFT ANY JOIN
(
    SELECT sum(age)::Nullable(Int64) AS age_sum, name, uid
    FROM users
    GROUP BY name, uid
) u2
ON u1.name = reverse(u2.name) AND u1.age = u2.uid
WHERE uid < age_sum;

EXPLAIN actions = 1, keep_logical_steps = 1
SELECT *
FROM users u1 LEFT ANY JOIN
(
    SELECT sum(age)::Nullable(Int64) AS age_sum, name, uid
    FROM users
    GROUP BY name, uid
) u2
ON u1.name = reverse(u2.name) AND u1.age < u2.uid
WHERE uid < age_sum;