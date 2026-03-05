-- Tags: no-fasttest
-- Test for https://github.com/ClickHouse/ClickHouse/issues/98847
-- Composing loop() with cluster table functions like urlCluster() should work
-- without throwing "Unexpected table function name: loop" exception.

SELECT c0 FROM loop(urlCluster('test_cluster_two_shards_localhost', 'http://localhost:8123/?query=SELECT+1', 'CSV', 'c0 Int')) LIMIT 1;
SELECT c0 FROM loop(urlCluster('test_cluster_two_shards_localhost', 'http://localhost:8123/?query=SELECT+1', 'CSV', 'c0 Int')) LIMIT 3;
