#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/108524
# DROP TABLE with iceberg_delete_data_on_drop=1 must remove the table's files so
# that a table can be re-created at the same path. Two bugs made this a no-op:
#   1. IcebergMetadata::drop listed files with the table path used as both the
#      listing base and the prefix, so it matched nothing.
#   2. The setting was read from the global context (drop runs in the background),
#      so it was always the default false.
# With the setting off (default) the files must be left in place.
# DROP ... SYNC forces the background drop to finish before the assertions.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
rm -rf "${TABLE_PATH}" 2>/dev/null

# --- Case 1: setting ON -> DROP removes the files -> re-create at same path works.
${CLICKHOUSE_CLIENT} --iceberg_delete_data_on_drop=1 --query "
    CREATE TABLE ${TABLE} (event_month Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}')
    PARTITION BY event_month
"
[ -f "${TABLE_PATH}metadata/v1.metadata.json" ] && echo "created: metadata present" || echo "created: metadata MISSING"

${CLICKHOUSE_CLIENT} --iceberg_delete_data_on_drop=1 --query "DROP TABLE ${TABLE} SYNC"
[ -z "$(ls -A "${TABLE_PATH}" 2>/dev/null)" ] && echo "drop on: table path empty" || echo "drop on: table path NOT empty"

# Re-creating a table at the same path must now succeed (previously TABLE_ALREADY_EXISTS).
if ${CLICKHOUSE_CLIENT} --iceberg_delete_data_on_drop=1 --query "
    CREATE TABLE ${TABLE} (event_month Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}')
    PARTITION BY event_month
" 2>/dev/null; then
    echo "recreate at same path: ok"
else
    echo "recreate at same path: failed"
fi
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"

# --- Case 2: setting off (default) -> DROP keeps the files.
rm -rf "${TABLE_PATH}" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (event_month Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}')
    PARTITION BY event_month
"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
[ -f "${TABLE_PATH}metadata/v1.metadata.json" ] && echo "drop off: metadata kept" || echo "drop off: metadata REMOVED"

# --- Case 3: an orphaned `metadata/version-hint.text` (no *.metadata.json) is an
# existing-table marker for createInitial. A partially failed DROP can leave the
# version hint behind after the metadata.json is gone; without treating it as a
# marker, CREATE writes a fresh v1.metadata.json, then fails the If-None-Match
# write of version-hint.text, and every retry then fails with TABLE_ALREADY_EXISTS.
rm -rf "${TABLE_PATH}" 2>/dev/null
mkdir -p "${TABLE_PATH}metadata"
echo "1" > "${TABLE_PATH}metadata/version-hint.text"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (event_month Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}')
    PARTITION BY event_month
    SETTINGS iceberg_use_version_hint = 1
" 2>&1 | grep -qF "TABLE_ALREADY_EXISTS" && echo "orphan hint: create rejected" || echo "orphan hint: create NOT rejected"
[ -f "${TABLE_PATH}metadata/v1.metadata.json" ] && echo "orphan hint: spurious metadata written" || echo "orphan hint: no spurious metadata"

rm -rf "${TABLE_PATH}" 2>/dev/null
