-- Tests that SYSTEM SYNC FILESYSTEM CACHE preserves the optional cache name
-- when the query is formatted. This matters for ON CLUSTER queries, where
-- the query is reformatted before being sent to other nodes.

SELECT formatQuery('SYSTEM SYNC FILESYSTEM CACHE');
SELECT formatQuery('SYSTEM SYNC FILESYSTEM CACHE ''my_cache''');
SELECT formatQuery('SYSTEM SYNC FILESYSTEM CACHE ''my_cache'' ON CLUSTER ''cluster''');
