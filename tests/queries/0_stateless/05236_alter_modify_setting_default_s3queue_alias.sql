-- Tags: no-fasttest, zookeeper
-- no-fasttest: the `S3Queue` engine is not built in the fast test.

-- `s3queue_processing_threads_num` is a compatibility name of `processing_threads_num`, so the change
-- and the reset of this command land on the same setting once the engine resolves the names. The
-- resets of a command are applied before its changes, so the change is what stays.

DROP TABLE IF EXISTS t_s3queue_default_reset;

CREATE TABLE t_s3queue_default_reset (x UInt64)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/t_s3queue_default_reset/', 'username', 'password', CSV)
SETTINGS mode = 'unordered', processing_threads_num = 5, keeper_path = '/clickhouse/{database}/t_s3queue_default_reset';

SELECT extract(create_table_query, 'processing_threads_num = (\\d+)')
FROM system.tables WHERE database = currentDatabase() AND name = 't_s3queue_default_reset';

ALTER TABLE t_s3queue_default_reset MODIFY SETTING s3queue_processing_threads_num = 8, processing_threads_num = DEFAULT;

SELECT extract(create_table_query, 'processing_threads_num = (\\d+)')
FROM system.tables WHERE database = currentDatabase() AND name = 't_s3queue_default_reset';

DROP TABLE t_s3queue_default_reset;
