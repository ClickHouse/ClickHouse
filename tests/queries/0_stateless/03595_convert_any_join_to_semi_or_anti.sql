SET enable_analyzer = 1;
SET query_plan_join_swap_table = false;
SET enable_parallel_replicas = 0;
SET correlated_subqueries_default_join_kind = 'left';

CREATE TABLE users1 (uid Int16, name String, age Int16) ENGINE=Memory;
INSERT INTO users1 SELECT number as uid, 'Alice' as name, 30 as age FROM numbers(100000);

CREATE TABLE users2 (uid Int16, name String, age Int16) ENGINE=Memory;
INSERT INTO users2 SELECT number as uid, 'Alice2' as name, 30 as age FROM numbers(1000);

EXPLAIN actions = 1
SELECT count()
FROM users1 u1
WHERE EXISTS (SELECT * FROM users2 u2 WHERE u1.uid != u2.uid);

EXPLAIN actions = 1
SELECT count()
FROM users1 u1
WHERE NOT EXISTS (SELECT * FROM users2 u2 WHERE u1.uid != u2.uid);
