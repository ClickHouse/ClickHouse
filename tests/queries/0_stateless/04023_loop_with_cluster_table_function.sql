-- Tags: no-fasttest
-- Test for https://github.com/ClickHouse/ClickHouse/issues/98847
-- Composing loop() with cluster table functions like urlCluster() should work
-- without throwing "Unexpected table function name: loop" exception.

SELECT c0 FROM loop(urlCluster('test_cluster_two_shards_localhost', 'http://localhost:8123/?query=SELECT+1', 'CSV', 'c0 Int')) LIMIT 1;
SELECT c0 FROM loop(urlCluster('test_cluster_two_shards_localhost', 'http://localhost:8123/?query=SELECT+1', 'CSV', 'c0 Int')) LIMIT 3;

-- Old planner path (enable_analyzer=0) uses InterpreterSelectWithUnionQuery in ReadFromLoopStep.
-- This branch is separate from the analyzer path and must be tested independently.
SELECT c0 FROM loop(urlCluster('test_cluster_two_shards_localhost', 'http://localhost:8123/?query=SELECT+1', 'CSV', 'c0 Int')) LIMIT 1 SETTINGS enable_analyzer=0;
SELECT c0 FROM loop(urlCluster('test_cluster_two_shards_localhost', 'http://localhost:8123/?query=SELECT+1', 'CSV', 'c0 Int')) LIMIT 3 SETTINGS enable_analyzer=0;
