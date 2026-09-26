-- Tags: no-fasttest
-- no-fasttest: needs an S3-backed disk so the part is written to object storage and issues S3 PUT requests.

DROP TABLE IF EXISTS t_packed_storage;

-- A packed part keeps the whole part in a single archive object, so writing it issues a single S3 PUT
-- regardless of how many columns/files the part has.
-- async_insert is set to 0 (it defaults to true) so the part is written synchronously within the INSERT
-- query and its S3PutObject events are attributed to it rather than to a background flush operation.
CREATE TABLE t_packed_storage (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS
    storage_policy = 's3_cache',
    min_bytes_for_full_part_storage = '1G',
    min_bytes_for_wide_part = '1G';

INSERT INTO t_packed_storage SETTINGS async_insert = 0 VALUES (1, 'foo');

DROP TABLE t_packed_storage;

CREATE TABLE t_packed_storage (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS
    storage_policy = 's3_cache',
    min_bytes_for_full_part_storage = '1G',
    min_bytes_for_wide_part = 0;

INSERT INTO t_packed_storage SETTINGS async_insert = 0 VALUES (1, 'foo');

DROP TABLE t_packed_storage;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['S3PutObject'] FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE 'INSERT INTO t_packed_storage%';
