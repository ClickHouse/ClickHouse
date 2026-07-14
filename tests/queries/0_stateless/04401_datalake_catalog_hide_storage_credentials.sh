#!/usr/bin/env bash
# Regression test: DataLake credential settings (legacy storage_* aliases and canonical names)
# must be redacted as [HIDDEN] when the CREATE query is formatted, for BOTH the DataLakeCatalog
# database engine (system.databases.engine_full, SHOW CREATE DATABASE) and the Iceberg*/Paimon*
# table engines (system.tables.engine_full, SHOW CREATE TABLE), since DataLakeStorageSettings is
# shared. Uses clickhouse-format so it needs no live catalog and is safe to run in parallel.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SECRET="SECRET_THAT_MUST_NOT_LEAK"

settings_list="catalog_type = 'rest', warehouse = 'demo',
storage_catalog_credential = '${SECRET}',
storage_auth_header = '${SECRET}',
storage_aws_access_key_id = '${SECRET}',
storage_aws_secret_access_key = '${SECRET}',
catalog_credential = '${SECRET}',
auth_header = '${SECRET}',
aws_access_key_id = '${SECRET}',
aws_secret_access_key = '${SECRET}'"

check_hidden() {
    local label="$1"
    local query="$2"
    echo "--- ${label}"
    local formatted
    formatted=$(echo "$query" | $CLICKHOUSE_FORMAT --oneline)
    for setting in \
        storage_catalog_credential \
        storage_auth_header \
        storage_aws_access_key_id \
        storage_aws_secret_access_key \
        catalog_credential \
        auth_header \
        aws_access_key_id \
        aws_secret_access_key
    do
        echo "$formatted" | grep -oE "(^|[, ])${setting} = '[^']*'" | sed -E "s/^[, ]//"
    done
    if echo "$formatted" | grep -q "$SECRET"; then
        echo "FAIL: secret leaked in formatted query"
    else
        echo "OK: no secret in formatted query"
    fi
}

# DataLakeCatalog database engine.
check_hidden "database engine DataLakeCatalog" \
    "CREATE DATABASE d ENGINE = DataLakeCatalog('http://example.invalid/catalog') SETTINGS ${settings_list}"

# Iceberg table engine (shares DataLakeStorageSettings via registerStorageObjectStorage).
check_hidden "table engine IcebergLocal" \
    "CREATE TABLE t (x Int32) ENGINE = IcebergLocal('/tmp/x') SETTINGS ${settings_list}"
