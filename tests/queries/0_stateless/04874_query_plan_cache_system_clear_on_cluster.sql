-- Tests that SYSTEM CLEAR QUERY PLAN CACHE parses and is correctly formatted (serialized to string),
-- including the deprecated DROP alias and the ON CLUSTER clause.
-- This is important for ON CLUSTER queries where the query is reformatted before being sent to other nodes.

SELECT formatQuery('SYSTEM CLEAR QUERY PLAN CACHE');

-- The DROP alias should be normalized to CLEAR
SELECT formatQuery('SYSTEM DROP QUERY PLAN CACHE');

-- ON CLUSTER should be preserved
SELECT formatQuery('SYSTEM CLEAR QUERY PLAN CACHE ON CLUSTER ''cluster''');
SELECT formatQuery('SYSTEM DROP QUERY PLAN CACHE ON CLUSTER ''cluster''');
