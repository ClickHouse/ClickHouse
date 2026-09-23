-- Tags: no-fasttest
-- - no-fasttest: the DataLakeCatalog database engine requires the USE_AVRO build option.

-- A BigLake `google_service_account_key` is validated when the catalog is built on CREATE, before any network request.

SET allow_database_iceberg = 1;

-- Not a JSON object.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://localhost:1/iceberg')
SETTINGS catalog_type = 'biglake', warehouse = 'gs://bucket', google_service_account_key = 'not a json'; -- { serverError INCORRECT_DATA }

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://localhost:1/iceberg')
SETTINGS catalog_type = 'biglake', warehouse = 'gs://bucket', google_service_account_key = '[]'; -- { serverError INCORRECT_DATA }

-- A JSON object without `client_email` and `private_key`.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://localhost:1/iceberg')
SETTINGS catalog_type = 'biglake', warehouse = 'gs://bucket', google_service_account_key = '{"type": "service_account"}'; -- { serverError BAD_ARGUMENTS }

-- A service account key and the ADC credentials are mutually exclusive.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://localhost:1/iceberg')
SETTINGS catalog_type = 'biglake', warehouse = 'gs://bucket',
    google_service_account_key = '{"type": "service_account", "client_email": "a@b.iam.gserviceaccount.com", "private_key": "x"}',
    google_adc_client_id = 'id', google_adc_client_secret = 'secret', google_adc_refresh_token = 'token'; -- { serverError BAD_ARGUMENTS }

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://localhost:1/iceberg')
SETTINGS catalog_type = 'biglake', warehouse = 'gs://bucket',
    google_service_account_key = '{"type": "service_account", "client_email": "a@b.iam.gserviceaccount.com", "private_key": "x"}',
    google_adc_refresh_token = 'token'; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
