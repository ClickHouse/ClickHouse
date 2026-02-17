-- Tags: no-parallel
-- Tests that SYSTEM DROP QUERY CACHE TAG is correctly formatted (serialized to string).
-- This is important for ON CLUSTER queries where the query is reformatted before being sent to other nodes.

-- Without TAG
SELECT formatQuery('SYSTEM DROP QUERY CACHE');

-- With TAG - it should be preserved
SELECT formatQuery('SYSTEM DROP QUERY CACHE TAG ''tag''');

-- With TAG and ON CLUSTER - both should be preserved
SELECT formatQuery('SYSTEM DROP QUERY CACHE TAG ''tag'' ON CLUSTER ''cluster''');

-- With ON CLUSTER but no TAG
SELECT formatQuery('SYSTEM DROP QUERY CACHE ON CLUSTER ''cluster''');
