-- Tags: no-fasttest

-- `catalog_type` (and related Glue/REST settings) belong to DataLakeCatalog databases,
-- not to Iceberg table engines. Previously this produced a bare UNKNOWN_SETTING.
CREATE TABLE t_04695 (x Int)
ENGINE = IcebergLocal('/tmp/clickhouse_04695_does_not_exist')
SETTINGS catalog_type = 'glue'; -- { serverError BAD_ARGUMENTS }

-- Same for the older table-level catalog setting name.
CREATE TABLE t_04695_deprecated (x Int)
ENGINE = IcebergLocal('/tmp/clickhouse_04695_does_not_exist')
SETTINGS storage_catalog_type = 'glue'; -- { serverError BAD_ARGUMENTS }
