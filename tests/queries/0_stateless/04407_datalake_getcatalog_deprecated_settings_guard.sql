-- Tags: no-fasttest

-- The deprecated data-lake setting `storage_catalog_url` must be rejected by the catalog guard.
-- Before the fix, the guard checked `storage_catalog_type` and `storage_aws_access_key_id` but not
-- `storage_catalog_url`, so this setting silently slipped through. `IcebergLocal` is used so the guard
-- (in DataLakeConfiguration::getCatalog) is reached during CREATE without any network/storage access.
CREATE TABLE t_04407 (x Int) ENGINE = IcebergLocal('/tmp/clickhouse_04407_does_not_exist') SETTINGS storage_catalog_url = 'http://example.invalid'; -- { serverError BAD_ARGUMENTS }
