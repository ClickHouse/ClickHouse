-- Tags: no-fasttest
-- Verify that loop() wrapping a cluster table function does not cause a logical error.

SELECT * FROM loop(urlCluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/a.tsv')) LIMIT 1 FORMAT Null;
SELECT * FROM loop(urlCluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/a.tsv', 'TSV')) LIMIT 1 FORMAT Null;
SELECT * FROM loop(urlCluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/a.tsv', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64')) LIMIT 1 FORMAT Null;
