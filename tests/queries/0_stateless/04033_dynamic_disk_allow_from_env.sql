-- Tags: no-fasttest

-- Verify that `from_env`, `include`, and `from_zk` in dynamic disk configuration
-- are blocked by default (server settings default to false).

DROP TABLE IF EXISTS test;

-- from_env is blocked by default
CREATE TABLE test (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(
    name = 'test_from_env',
    type = object_storage,
    object_storage_type = s3,
    endpoint = 'from_env S3_ENDPOINT',
    access_key_id = clickhouse,
    secret_access_key = clickhouse); -- { serverError ACCESS_DENIED }

-- include is blocked by default
CREATE TABLE test (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = s3,
    include = 'some_include',
    access_key_id = clickhouse,
    secret_access_key = clickhouse); -- { serverError ACCESS_DENIED }

-- from_zk is blocked by default
CREATE TABLE test (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = s3,
    endpoint = 'from_zk /some/zk/path',
    access_key_id = clickhouse,
    secret_access_key = clickhouse); -- { serverError ACCESS_DENIED }

DROP TABLE IF EXISTS test;
