-- Tags: no-fasttest

DROP TABLE IF EXISTS t_packed_storage;

CREATE TABLE t_packed_storage (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_full_part_storage = '1G',
    min_bytes_for_wide_part = '1G';

INSERT INTO t_packed_storage VALUES (1, 'foo');

DROP TABLE t_packed_storage;

CREATE TABLE t_packed_storage (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_full_part_storage = '1G',
    min_bytes_for_wide_part = 0;

INSERT INTO t_packed_storage VALUES (1, 'foo');

DROP TABLE t_packed_storage;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['S3PutObject'] FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE 'INSERT INTO t_packed_storage%';
