#!/usr/bin/env bash
# Tags: no-fasttest

# Server may ignore some exceptions, but it still print exceptions to logs and (at least in CI) sends Error and Warning log messages to client
# making test fail because of non-empty stderr. Ignore such log messages.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NEW_DB_NAME="${CLICKHOUSE_DATABASE}_03913_DATALKE"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${NEW_DB_NAME};"
$CLICKHOUSE_CLIENT -q "
SET allow_experimental_database_iceberg = 1;
CREATE DATABASE ${NEW_DB_NAME}
ENGINE = DataLakeCatalog('http://rest:8181/v1', 'admin', 'password')
SETTINGS 
    catalog_type = 'rest', 
    auth_header = 'wrong.header',
    storage_endpoint = 'http://minio:9000/lakehouse', 
    warehouse = 'demo';
"

$CLICKHOUSE_CLIENT -q "
select database || '.' || name FROM system.tables where database = '${NEW_DB_NAME}' and engine = 'MergeTree'
SETTINGS show_data_lake_catalogs_in_system_tables = 1;
"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${NEW_DB_NAME};"
