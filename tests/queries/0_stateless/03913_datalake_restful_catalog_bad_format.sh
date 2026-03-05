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
" 2>&1 | grep -o 'Invalid auth header format'

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '${NEW_DB_NAME}';"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${NEW_DB_NAME};"

# Verify that a forbidden header (configured via http_forbid_headers) is rejected at CREATE DATABASE time,
# The values for exact_header are defined as forbidden in tests/config/config.d/forbidden_headers.xml.
NEW_DB_FORBIDDEN="${CLICKHOUSE_DATABASE}_03913_FORBIDDEN"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${NEW_DB_FORBIDDEN};"
$CLICKHOUSE_CLIENT -q "
SET allow_experimental_database_iceberg = 1;
CREATE DATABASE ${NEW_DB_FORBIDDEN}
ENGINE = DataLakeCatalog('http://localhost:8181/v1')
SETTINGS
    catalog_type = 'rest',
    auth_header = 'exact_header: some_value',
    warehouse = 'demo';
" 2>&1 | grep -o 'is forbidden'

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '${NEW_DB_FORBIDDEN}';"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${NEW_DB_FORBIDDEN};"