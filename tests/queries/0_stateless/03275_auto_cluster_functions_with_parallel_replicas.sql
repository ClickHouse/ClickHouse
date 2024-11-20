-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SET enable_parallel_replicas=1;
SET cluster_for_parallel_replicas='default';
SET parallel_replicas_for_cluster_engines=true;

EXPLAIN SELECT * FROM url('http://localhost:8123');
EXPLAIN SELECT * FROM s3('http://localhost:11111/test/a.tsv', 'TSV');

SET parallel_replicas_for_cluster_engines=false;

EXPLAIN SELECT * FROM url('http://localhost:8123');
EXPLAIN SELECT * FROM s3('http://localhost:11111/test/a.tsv', 'TSV');
