-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Minio

SET enable_analyzer=1;
SET enable_parallel_replicas=1;
SET max_parallel_replicas=4;
SET cluster_for_parallel_replicas='test_cluster_two_shards';
SET parallel_replicas_for_cluster_engines=true;
SET query_plan_join_swap_table=0;

EXPLAIN SELECT * FROM url('http://localhost:8123');
EXPLAIN SELECT * FROM s3('http://localhost:11111/test/a.tsv', 'TSV');
EXPLAIN SELECT * FROM s3('http://localhost:11111/test/a.tsv', 'TSV') where c1 in (SELECT c1 FROM s3('http://localhost:11111/test/a.tsv', 'TSV'));
EXPLAIN SELECT sum(c1) FROM (SELECT * FROM s3('http://localhost:11111/test/a.tsv', 'TSV'));
EXPLAIN SELECT number FROM system.numbers n JOIN s3('http://localhost:11111/test/a.tsv', 'TSV') s ON (toInt64(n.number) = toInt64(s.c1));
EXPLAIN SELECT number FROM system.numbers n JOIN (SELECT * FROM s3('http://localhost:11111/test/a.tsv', 'TSV')) s ON (toInt64(n.number) = toInt64(s.c1));

SET parallel_replicas_for_cluster_engines=false;

EXPLAIN SELECT * FROM url('http://localhost:8123');
EXPLAIN SELECT * FROM s3('http://localhost:11111/test/a.tsv', 'TSV');
