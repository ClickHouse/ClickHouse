-- Tags: no-fasttest, zookeeper
-- no-fasttest: the queue engines are not built in the fast test.

-- `system.s3_queue_settings` and `system.azure_queue_settings` hide a secret setting value as `SHOW CREATE TABLE` does.
-- Each engine takes both secrets.

DROP TABLE IF EXISTS t_s3queue;
DROP TABLE IF EXISTS t_azurequeue;
DROP TABLE IF EXISTS t_s3queue_unset;

CREATE TABLE t_s3queue (a String)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/t_s3queue/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered',
    after_processing_move_access_key_id = 'visible_key_id',
    after_processing_move_secret_access_key = 'SEKRIT_05331_1',
    after_processing_move_connection_string = 'DefaultEndpointsProtocol=https;AccountName=a;AccountKey=SEKRIT_05331_2;';

CREATE TABLE t_azurequeue (a String)
ENGINE = AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=SEKRIT_05331_3;BlobEndpoint=http://127.0.0.1:1/devstoreaccount1;', 'cont', 't_azurequeue/*', 'CSV')
SETTINGS mode = 'unordered',
    after_processing_move_secret_access_key = 'SEKRIT_05331_4',
    after_processing_move_connection_string = 'DefaultEndpointsProtocol=https;AccountName=a;AccountKey=SEKRIT_05331_5;',
    after_processing_move_container = 'visible_container';

CREATE TABLE t_s3queue_unset (a String)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/t_s3queue_unset/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered';

SELECT table, name, value FROM system.s3_queue_settings
WHERE database = currentDatabase() AND name LIKE 'after_processing_move_%' AND value != '' ORDER BY table, name;

SELECT table, name, value FROM system.azure_queue_settings
WHERE database = currentDatabase() AND name LIKE 'after_processing_move_%' AND value != '' ORDER BY table, name;

-- An unset secret is empty, not hidden.
SELECT table, name, value, changed FROM system.s3_queue_settings
WHERE database = currentDatabase() AND table = 't_s3queue_unset'
    AND name IN ('after_processing_move_secret_access_key', 'after_processing_move_connection_string') ORDER BY name;

DROP TABLE t_s3queue;
DROP TABLE t_azurequeue;
DROP TABLE t_s3queue_unset;
