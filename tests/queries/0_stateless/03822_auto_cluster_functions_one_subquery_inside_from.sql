-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Minio

SET enable_analyzer=1;
SET enable_parallel_replicas=1;
SET max_parallel_replicas=4;
SET cluster_for_parallel_replicas='test_cluster_two_shards';
SET query_plan_join_swap_table=0;
SET enable_join_runtime_filters=0;


SET parallel_replicas_for_cluster_engines=true;

EXPLAIN SELECT * FROM (SELECT * FROM s3('http://localhost:11111/test/a.tsv', 'TSV'));
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV');


SET parallel_replicas_for_cluster_engines=false;

EXPLAIN SELECT * FROM (SELECT * FROM s3('http://localhost:11111/test/a.tsv', 'TSV'));
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV');
