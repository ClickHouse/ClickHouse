#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Catalog settings belong to DataLakeCatalog databases, not to Iceberg/DeltaLake table engines.
# Passing them to a table used to produce a bare UNKNOWN_SETTING (unprefixed names) or was silently
# ignored (deprecated storage_* aliases). Assert both the BAD_ARGUMENTS code and the guidance message.

# Unprefixed database catalog settings (`catalog_type`, and one not in the original hand-picked subset).
for setting in "catalog_type = 'glue'" "warehouse = 'wh'"; do
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04695 (x Int) ENGINE = IcebergLocal('/tmp/ch_04695_does_not_exist') SETTINGS ${setting}" 2>&1 \
        | grep -oF "is a database engine setting for DataLakeCatalog" | head -n1
done

# Deprecated table-level storage_* catalog aliases (the wired-up alias, plus one that used to slip through).
for setting in "storage_catalog_type = 'glue'" "storage_region = 'us-east-1'"; do
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04695 (x Int) ENGINE = IcebergLocal('/tmp/ch_04695_does_not_exist') SETTINGS ${setting}" 2>&1 \
        | grep -oF "catalog configuration settings and are no longer supported" | head -n1
done
