#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/105660.
# Once `version-hint.text` exists, any writer to the same Iceberg table must
# keep it in sync, even when the writer was not given `iceberg_use_version_hint`.
# Otherwise a subsequent reader using the hint observes stale data.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

# Step 1: a writer with `iceberg_use_version_hint = 1` creates the table and
# the `version-hint.text` file.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}')
    SETTINGS iceberg_use_version_hint = 1
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1)"

echo "After INSERT via named table:"
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE} ORDER BY c0"
echo "version-hint.text: $(cat "${TABLE_PATH}metadata/version-hint.text")"

# Step 2: a separate writer that does NOT have `iceberg_use_version_hint`
# advances the table via the `icebergLocal` table function. Before the fix,
# this writer would not touch `version-hint.text`, so the named table above
# would keep reading the previous version on subsequent SELECTs.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO TABLE FUNCTION icebergLocal('${TABLE_PATH}', 'Parquet', 'c0 Int32') (c0) VALUES (2)
"

echo "After INSERT via table function, version-hint.text: $(cat "${TABLE_PATH}metadata/version-hint.text")"

echo "SELECT via named table (uses version-hint):"
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE} ORDER BY c0"

# Confirm the table function reader (which uses listing-based discovery) sees
# the same rows: this rules out a divergence between the two read paths.
echo "SELECT via table function:"
${CLICKHOUSE_CLIENT} --query "
    SELECT c0 FROM icebergLocal('${TABLE_PATH}', 'Parquet') ORDER BY c0
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}" 2>/dev/null

echo "OK"
