SET enable_analyzer = 1;
SET analyzer_inline_views = 1;

SET enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 0;

CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=MergeTree() ORDER BY ();
CREATE VIEW uv (name String, age Int16) AS SELECT name, age FROM users;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

EXPLAIN QUERY TREE
SELECT name, sum(age)
FROM uv
WHERE name > ''
GROUP BY name
ORDER BY name;

EXPLAIN
SELECT name, sum(age)
FROM uv
WHERE name > ''
GROUP BY name
ORDER BY name;

SELECT name, sum(age)
FROM uv
WHERE name > ''
GROUP BY name
ORDER BY name;
