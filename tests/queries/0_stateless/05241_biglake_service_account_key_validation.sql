-- Tags: no-fasttest
-- - no-fasttest: the DataLakeCatalog database engine requires the USE_AVRO build option.

-- A BigLake `google_service_account_key` and its combination with other settings are validated on CREATE, before any network request.

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

-- `disk` would read the data files with the credentials of the disk instead of the Google credentials of the catalog.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://localhost:1/iceberg')
SETTINGS catalog_type = 'biglake', warehouse = 'gs://bucket', disk = 'default',
    google_service_account_key = '{"type": "service_account", "client_email": "a@b.iam.gserviceaccount.com", "private_key": "x"}'; -- { serverError BAD_ARGUMENTS }

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://localhost:1/iceberg')
SETTINGS catalog_type = 'biglake', warehouse = 'gs://bucket', disk = 'default',
    google_adc_client_id = 'id', google_adc_client_secret = 'secret', google_adc_refresh_token = 'token'; -- { serverError BAD_ARGUMENTS }

-- Without Google credentials `disk` is accepted. `ATTACH` does not connect to the catalog, so the endpoint does not have to exist.
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://localhost:1/iceberg')
SETTINGS catalog_type = 'biglake', warehouse = 'gs://bucket', disk = 'default';
SELECT count() FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT count() FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
