-- Plan-based parallel replicas keeps joins logical until after `applyParallelReplicas`, while
-- `optimizeJoinLazyIndexing` only recognizes a physical `JoinStep`. The pass therefore runs after the
-- deferred conversion, so joins that stay local (here: FULL JOIN, which is never distributed) keep the
-- lazy column indexing they get without parallel replicas. Whether the optimization fired cannot be
-- observed from SQL - it is not shown by EXPLAIN and has no ProfileEvents counter - so this test pins the
-- results of the reordered pass instead. See PR #112268 review (comment r3675370319).

DROP TABLE IF EXISTS lzi_left SYNC;
DROP TABLE IF EXISTS lzi_right SYNC;

CREATE TABLE lzi_left  (a UInt64, b UInt64, c String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE lzi_right (a UInt64, b UInt64, c String) ENGINE = MergeTree ORDER BY a;
INSERT INTO lzi_left  SELECT number, number, toString(number) FROM numbers(10000);
INSERT INTO lzi_right SELECT number, 50, toString(number) FROM numbers(100);

SET enable_analyzer = 1;
SET query_plan_min_columns_for_join_lazy_indexing = 1;
SET query_plan_optimize_join_order_limit = 1;
SET query_plan_join_swap_table = 'false';
SET join_algorithm = 'hash';
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- FULL JOIN is kept local, so it is converted to a physical join only after the fragment is built.
SELECT 'FULL + LIMIT', count() FROM (
    SELECT * FROM lzi_left FULL JOIN lzi_right ON lzi_left.a = lzi_right.a LIMIT 5
);

SELECT 'FULL + ORDER BY + LIMIT', count() FROM (
    SELECT * FROM lzi_left FULL JOIN lzi_right ON lzi_left.a = lzi_right.a ORDER BY lzi_left.a LIMIT 5
);

-- An INNER join is distributed, so the same shapes also cover the shipped-fragment path.
SELECT 'INNER + LIMIT', count() FROM (
    SELECT * FROM lzi_left JOIN lzi_right ON lzi_left.a = lzi_right.a LIMIT 5
);

SELECT 'INNER filtered', count(), sum(lzi_left.b) FROM
    lzi_left JOIN lzi_right ON lzi_left.a = lzi_right.a
WHERE lzi_left.b < lzi_right.b;

DROP TABLE lzi_left SYNC;
DROP TABLE lzi_right SYNC;
