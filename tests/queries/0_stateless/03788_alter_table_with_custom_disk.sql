-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: Depends on S3
-- Tag no-replicated-database: plain rewritable should not be shared between replicas

DROP TABLE IF EXISTS 03788_custom_disk;

CREATE TABLE 03788_custom_disk (field UInt64) ENGINE = MergeTree ORDER BY field
SETTINGS disk = disk(
    name = 03788_plain_rewritable,
    type = s3_plain_rewritable,
    endpoint = 'http://localhost:11111/test/03788_custom_disk/',
    access_key_id = clickhouse,
    secret_access_key = clickhouse
);

ALTER TABLE 03788_custom_disk MODIFY COMMENT 'not supported'; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE 03788_custom_disk MODIFY SETTING min_bytes_for_wide_part = 1; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE 03788_custom_disk;
