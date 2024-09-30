DROP TABLE IF EXISTS merge_tree_in_subqueries;
CREATE TABLE merge_tree_in_subqueries (id UInt64, name String, num UInt64) ENGINE = MergeTree ORDER BY (id, name);
INSERT INTO merge_tree_in_subqueries VALUES(1, 'test1', 42);
INSERT INTO merge_tree_in_subqueries VALUES(2, 'test2', 8);
INSERT INTO merge_tree_in_subqueries VALUES(3, 'test3', 8);
INSERT INTO merge_tree_in_subqueries VALUES(4, 'test4', 1985);
INSERT INTO merge_tree_in_subqueries VALUES(5, 'test5', 0);

SET max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree=1;

SELECT * FROM merge_tree_in_subqueries WHERE id IN (SELECT * FROM system.numbers LIMIT 0) SETTINGS enable_parallel_replicas=2, parallel_replicas_allow_in_with_subquery=0; -- { serverError SUPPORT_IS_DISABLED }
SELECT * FROM merge_tree_in_subqueries WHERE id IN (SELECT * FROM system.numbers LIMIT 0) SETTINGS enable_parallel_replicas=2, parallel_replicas_allow_in_with_subquery=1;
SELECT * FROM merge_tree_in_subqueries WHERE id IN (SELECT * FROM system.numbers LIMIT 0) SETTINGS enable_parallel_replicas=1;

SELECT '---';
SELECT * FROM merge_tree_in_subqueries WHERE id IN (SELECT * FROM system.numbers LIMIT 2, 3) ORDER BY id SETTINGS enable_parallel_replicas=2, parallel_replicas_allow_in_with_subquery=0; -- { serverError SUPPORT_IS_DISABLED };
SELECT * FROM merge_tree_in_subqueries WHERE id IN (SELECT * FROM system.numbers LIMIT 2, 3) ORDER BY id SETTINGS enable_parallel_replicas=2, parallel_replicas_allow_in_with_subquery=1;
SELECT * FROM merge_tree_in_subqueries WHERE id IN (SELECT * FROM system.numbers LIMIT 2, 3) ORDER BY id SETTINGS enable_parallel_replicas=1;

SELECT '---';
SELECT * FROM merge_tree_in_subqueries WHERE id IN (SELECT 1) ORDER BY id SETTINGS enable_parallel_replicas=2, parallel_replicas_allow_in_with_subquery=0; -- { serverError SUPPORT_IS_DISABLED };
SELECT * FROM merge_tree_in_subqueries WHERE id IN (SELECT 1) ORDER BY id SETTINGS enable_parallel_replicas=2, parallel_replicas_allow_in_with_subquery=1;
SELECT * FROM merge_tree_in_subqueries WHERE id IN (SELECT 1) ORDER BY id SETTINGS enable_parallel_replicas=1;

-- IN with tuples is allowed
SELECT '---';
SELECT id, name FROM merge_tree_in_subqueries WHERE (id, name)  IN (3, 'test3') SETTINGS enable_parallel_replicas=2, parallel_replicas_allow_in_with_subquery=0;
SELECT id, name FROM merge_tree_in_subqueries WHERE (id, name)  IN (3, 'test3') SETTINGS enable_parallel_replicas=2, parallel_replicas_allow_in_with_subquery=1;

DROP TABLE IF EXISTS merge_tree_in_subqueries;
