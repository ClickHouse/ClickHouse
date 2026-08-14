#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/85413
# After DETACH + ATTACH (or server restart), an `IcebergLocal` table must
# remain writable. Previously the table-engine layer passed `is_readonly=true`
# to `StorageLocalConfiguration::createObjectStorage` on ATTACH to suppress
# remote-side initialization side effects, but `LocalObjectStorage` interpreted
# the flag as a permanent read-only mode and refused all subsequent writes
# with "Local object storage `Local` is readonly. (READONLY)".

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int)
    ENGINE = IcebergLocal('${TABLE_PATH}')
"

# Baseline: writes work on the freshly created table.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

# DETACH + ATTACH simulates a server restart by reloading the table.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${TABLE}"

# Without the fix, this INSERT throws READONLY.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (2)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
