-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Minio

SET enable_analyzer=1;
SET enable_parallel_replicas=1;
SET max_parallel_replicas=4;
SET cluster_for_parallel_replicas='test_cluster_two_shards';
SET parallel_replicas_for_cluster_engines=true;

EXPLAIN SELECT * FROM url('http://localhost:8123');
EXPLAIN SELECT * FROM s3('http://localhost:11111/test/a.tsv', 'TSV');
EXPLAIN SELECT * FROM s3('http://localhost:11111/test/a.tsv', 'TSV') where c1 in (SELECT c1 FROM s3('http://localhost:11111/test/a.tsv', 'TSV'));

SET parallel_replicas_for_cluster_engines=false;

EXPLAIN SELECT * FROM url('http://localhost:8123');
EXPLAIN SELECT * FROM s3('http://localhost:11111/test/a.tsv', 'TSV');
