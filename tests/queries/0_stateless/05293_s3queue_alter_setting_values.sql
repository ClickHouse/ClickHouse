-- Tags: no-fasttest, zookeeper
-- no-fasttest: the `S3Queue` engine is not built in the fast test.

-- `ALTER TABLE ... MODIFY SETTING` of an `S3Queue` table converts and validates the new settings as `ATTACH` does,
-- before anything is changed: a quoted number is accepted as in `CREATE`, and a statement with a value that
-- `CREATE` rejects leaves Keeper and the table definition as they were.

DROP TABLE IF EXISTS t_s3queue_alter_values;

CREATE TABLE t_s3queue_alter_values (x UInt64)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/t_s3queue_alter_values/', 'username', 'password', CSV)
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/t_s3queue_alter_values', processing_threads_num = 2;

ALTER TABLE t_s3queue_alter_values MODIFY SETTING
    polling_min_timeout_ms = '20000', after_processing_retries = '5', enable_hash_ring_filtering = 'true',
    loading_retries = '7', cleanup_interval_min_ms = '30000';

SELECT name, value FROM system.s3_queue_settings
WHERE database = currentDatabase() AND table = 't_s3queue_alter_values'
    AND name IN ('polling_min_timeout_ms', 'after_processing_retries', 'enable_hash_ring_filtering', 'loading_retries', 'cleanup_interval_min_ms')
ORDER BY name;

ALTER TABLE t_s3queue_alter_values MODIFY SETTING after_processing = 'delete', polling_min_timeout_ms = 'abc'; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
ALTER TABLE t_s3queue_alter_values MODIFY SETTING loading_retries = 3, max_processed_files_before_commit = -1; -- { serverError CANNOT_CONVERT_TYPE }
ALTER TABLE t_s3queue_alter_values MODIFY SETTING processing_threads_num = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_s3queue_alter_values MODIFY SETTING after_processing = 'tag'; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_s3queue_alter_values MODIFY SETTING cleanup_interval_max_ms = 40000, cleanup_interval_min_ms = 40001; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_s3queue_alter_values MODIFY SETTING cleanup_interval_min_ms = 5000000000; -- { serverError CANNOT_CONVERT_TYPE }

SELECT JSONExtractString(value, 'after_processing'), JSONExtractUInt(value, 'loading_retries'), JSONExtractUInt(value, 'processing_threads_num')
FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/t_s3queue_alter_values' AND name = 'metadata';

SELECT name, value FROM system.s3_queue_settings
WHERE database = currentDatabase() AND table = 't_s3queue_alter_values'
    AND name IN ('cleanup_interval_max_ms', 'cleanup_interval_min_ms')
ORDER BY name;

DETACH TABLE t_s3queue_alter_values;
ATTACH TABLE t_s3queue_alter_values;

SELECT name, value FROM system.s3_queue_settings
WHERE database = currentDatabase() AND table = 't_s3queue_alter_values'
    AND name IN ('after_processing', 'cleanup_interval_max_ms', 'cleanup_interval_min_ms', 'loading_retries', 'max_processed_files_before_commit', 'polling_min_timeout_ms', 'processing_threads_num')
ORDER BY name;

DROP TABLE t_s3queue_alter_values;

-- Two tables with one `keeper_path` share the cleanup bounds, so an `ALTER` of one table can invert them.
CREATE TABLE t_s3queue_alter_values_a (x UInt64)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/t_s3queue_alter_values_shared/', 'username', 'password', CSV)
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/t_s3queue_alter_values_shared', cleanup_interval_min_ms = 100, cleanup_interval_max_ms = 100;

CREATE TABLE t_s3queue_alter_values_b (x UInt64)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/t_s3queue_alter_values_shared/', 'username', 'password', CSV)
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/t_s3queue_alter_values_shared';

ALTER TABLE t_s3queue_alter_values_b MODIFY SETTING cleanup_interval_min_ms = 101;
SELECT sleep(1) FORMAT Null;

SELECT name, value FROM system.s3_queue_settings
WHERE database = currentDatabase() AND table = 't_s3queue_alter_values_b'
    AND name IN ('cleanup_interval_max_ms', 'cleanup_interval_min_ms')
ORDER BY name;

DROP TABLE t_s3queue_alter_values_b;
DROP TABLE t_s3queue_alter_values_a;

-- The `ordered` mode does not allow changing these settings: a value equal to the current one in another spelling is not a change.
CREATE TABLE t_s3queue_alter_values_o (x UInt64)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/t_s3queue_alter_values_o/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', keeper_path = '/clickhouse/{database}/t_s3queue_alter_values_o', processing_threads_num = 2, enable_hash_ring_filtering = 0, s3queue_enable_logging_to_s3queue_log = 1;

ALTER TABLE t_s3queue_alter_values_o MODIFY SETTING processing_threads_num = '2', enable_hash_ring_filtering = false;
ALTER TABLE t_s3queue_alter_values_o MODIFY SETTING processing_threads_num = 2;
ALTER TABLE t_s3queue_alter_values_o MODIFY SETTING s3queue_enable_logging_to_s3queue_log = '1';
ALTER TABLE t_s3queue_alter_values_o MODIFY SETTING processing_threads_num = '3'; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_s3queue_alter_values_o MODIFY SETTING s3queue_enable_logging_to_s3queue_log = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT name, value FROM system.s3_queue_settings
WHERE database = currentDatabase() AND table = 't_s3queue_alter_values_o'
    AND name IN ('enable_hash_ring_filtering', 'enable_logging_to_queue_log', 'processing_threads_num')
ORDER BY name;

DROP TABLE t_s3queue_alter_values_o;
