-- Tags: no-fasttest

-- The credentials in the disk description of `mergeTreeParts` are hidden in the query log.

SELECT count() FROM mergeTreeParts(
    structure('id Int64'),
    parts(),
    disk(type = s3, metadata_type = plain, endpoint = 'http://localhost:11111/test/05341/', access_key_id = 'test', secret_access_key = 'testtest'),
    table_settings(index_granularity_bytes = 10485760));

SYSTEM FLUSH LOGS query_log;

SELECT query LIKE '%disk(type = s3, metadata_type = \'[HIDDEN]\', endpoint = \'[HIDDEN]\', access_key_id = \'[HIDDEN]\', secret_access_key = \'[HIDDEN]\')%', query LIKE '%testtest%'
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE '%mergeTreeParts(%' AND query NOT LIKE '%system.query_log%';
