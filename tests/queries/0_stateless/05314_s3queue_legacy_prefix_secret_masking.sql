-- Tags: no-fasttest, zookeeper
-- no-fasttest: the `S3Queue` engine is not built in the fast test.

-- The queue engines also take every setting with the legacy `s3queue_` prefix, and a secret written that way is
-- hidden in `SHOW CREATE TABLE`, `system.tables` and `system.query_log` as it is without the prefix.

DROP TABLE IF EXISTS t_legacy;
DROP TABLE IF EXISTS t_canonical;

CREATE TABLE t_legacy (a String)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/t_legacy/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered', s3queue_loading_retries = 7,
    s3queue_after_processing_move_secret_access_key = 'SEKRIT_05314_1',
    s3queue_after_processing_move_connection_string = 'DefaultEndpointsProtocol=https;AccountName=a;AccountKey=SEKRIT_05314_2;',
    s3queue_format_avro_schema_registry_url = 'http://user:SEKRIT_05314_3@registry:8080/';

CREATE TABLE t_canonical (a String)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/t_canonical/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered',
    after_processing_move_secret_access_key = 'SEKRIT_05314_4',
    after_processing_move_connection_string = 'DefaultEndpointsProtocol=https;AccountName=a;AccountKey=SEKRIT_05314_5;',
    format_avro_schema_registry_url = 'http://user:SEKRIT_05314_6@registry:8080/';

SHOW CREATE TABLE t_legacy;
SHOW CREATE TABLE t_canonical;

SELECT name, position(create_table_query, 'SEKRIT') = 0, position(engine_full, 'SEKRIT') = 0
FROM system.tables WHERE database = currentDatabase() ORDER BY name;

ALTER TABLE t_legacy MODIFY SETTING s3queue_after_processing_move_secret_access_key = 'SEKRIT_05314_7';

SYSTEM FLUSH LOGS query_log;
SELECT query_kind, position(query, 'SEKRIT') = 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND is_initial_query
    AND type = 'QueryFinish' AND query_kind IN ('Create', 'Alter')
ORDER BY event_time_microseconds;

DROP TABLE t_legacy;
DROP TABLE t_canonical;
