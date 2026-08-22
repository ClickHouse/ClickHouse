-- derivative of 02883_array_scalar_mult_div_modulo

SET serialize_query_plan=1;
SET enable_parallel_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree=1;

DROP TABLE IF EXISTS my_table;
CREATE TABLE my_table (values Array(Int32)) ENGINE = MergeTree() ORDER BY values;
INSERT INTO my_table (values) VALUES ([12, 3, 1]), ([5, 3, 1]);
SELECT values FROM my_table WHERE arrayExists(x -> x > 5, values);
DROP TABLE my_table;
