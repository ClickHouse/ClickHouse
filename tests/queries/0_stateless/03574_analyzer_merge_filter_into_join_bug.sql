SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_parallel_replicas = 0;

-- Workaround for `DB::Exception: Unexpected number of columns in result sample block: 4 expected 3 ([__table1, __table1, __table3.name, __table3.age] = [__table1] + [__table3.age] + [__table3.name]): While executing JoiningTransform. (LOGICAL_ERROR)`
SET query_plan_join_swap_table = 0;

CREATE TABLE users (uid Int16, name String, age Int16) ORDER BY uid;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

SELECT name, (SELECT count() FROM numbers(50) WHERE number = age)
FROM users
ORDER BY name;
