#!/usr/bin/env bash
# Regression test: legacy storage_* credential settings of the DataLakeCatalog database
# engine must be redacted as [HIDDEN] when the CREATE DATABASE query is formatted
# (system.databases.engine_full, SHOW CREATE DATABASE, query log). Uses clickhouse-format
# so it needs no live catalog and is safe to run in parallel.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SECRET="SECRET_THAT_MUST_NOT_LEAK"

query="CREATE DATABASE d ENGINE = DataLakeCatalog('http://example.invalid/catalog') SETTINGS
catalog_type = 'rest', warehouse = 'demo',
storage_catalog_credential = '${SECRET}',
storage_auth_header = '${SECRET}',
storage_aws_access_key_id = '${SECRET}',
storage_aws_secret_access_key = '${SECRET}',
catalog_credential = '${SECRET}',
auth_header = '${SECRET}',
aws_access_key_id = '${SECRET}',
aws_secret_access_key = '${SECRET}'"

formatted=$(echo "$query" | $CLICKHOUSE_FORMAT --oneline)

# Each credential setting (legacy storage_* aliases and canonical names) must be hidden.
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

# The secret value must not appear anywhere in the formatted output.
if echo "$formatted" | grep -q "$SECRET"; then
    echo "FAIL: secret leaked in formatted query"
else
    echo "OK: no secret in formatted query"
fi
