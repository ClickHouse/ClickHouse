-- Tests that SYSTEM DROP QUERY CACHE TAG is correctly formatted (serialized to string).
-- This is important for ON CLUSTER queries where the query is reformatted before being sent to other nodes.

-- Basic formatting without TAG
EXPLAIN SYNTAX SYSTEM DROP QUERY CACHE;

-- Formatting with TAG - the TAG should be preserved
EXPLAIN SYNTAX SYSTEM DROP QUERY CACHE TAG 'my_tag';

-- Formatting with TAG and ON CLUSTER - both should be preserved
EXPLAIN SYNTAX SYSTEM DROP QUERY CACHE TAG 'my_tag' ON CLUSTER 'test_shard_localhost';

-- Formatting with only ON CLUSTER (no TAG)
EXPLAIN SYNTAX SYSTEM DROP QUERY CACHE ON CLUSTER 'test_shard_localhost';
