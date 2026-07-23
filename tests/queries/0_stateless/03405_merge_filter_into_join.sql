CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

-- For some reason planner sometimes decides to swap tables.
-- It breaks test because it prints query plan with actions.
set query_plan_join_swap_table = 0;
set enable_analyzer = 1; -- Optimization requires LogicalJoinStep
set enable_parallel_replicas = 0; -- Optimization requires LogicalJoinStep
set parallel_hash_join_threshold = 0;

-- { echoOn }

EXPLAIN PLAN actions = 1
SELECT * FROM (SELECT * FROM users u1 INNER JOIN users u2 ON 1) WHERE age = u2.age ORDER BY ALL;
SELECT * FROM (SELECT * FROM users u1 INNER JOIN users u2 ON 1) WHERE age = u2.age ORDER BY ALL;

EXPLAIN PLAN actions = 1
SELECT * FROM (SELECT * FROM users u1 CROSS JOIN users u2) WHERE age = u2.age ORDER BY ALL;
SELECT * FROM (SELECT * FROM users u1 CROSS JOIN users u2) WHERE age = u2.age ORDER BY ALL;

EXPLAIN PLAN actions = 1
SELECT * FROM (SELECT * FROM users u1 SEMI JOIN users u2 ON 1) WHERE age = u2.age ORDER BY ALL;
EXPLAIN PLAN actions = 1
SELECT * FROM (SELECT * FROM users u1 FULL JOIN users u2 ON 1) WHERE age = u2.age ORDER BY ALL;
EXPLAIN PLAN actions = 1
SELECT * FROM (SELECT * FROM users u1 ANTI JOIN users u2 ON 1) WHERE age = u2.age ORDER BY ALL;
