#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Pins the refusal of Iceberg data compaction in the open-source build: with
# `allow_experimental_iceberg_compaction`, `OPTIMIZE TABLE` on an Iceberg table reports
# `NOT_IMPLEMENTED`, and without it the error names the setting. Cloud decides from the
# table-level setting instead of the one sent with the query, so there `OPTIMIZE` may either
# succeed or name the setting, and it must succeed on a table created with the setting enabled.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
ENABLED_TABLE="${TABLE}_enabled"
ENABLED_TABLE_PATH="${USER_FILES_PATH}/${ENABLED_TABLE}/"

trap 'rm -rf "${TABLE_PATH}" "${ENABLED_TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1)"

is_cloud=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

out=$(${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 \
    --query "OPTIMIZE TABLE ${TABLE}" 2>&1)

if grep -qF 'Logical error' <<< "$out"; then
    echo "FAIL: logical error: $out"
elif [ "$is_cloud" = 1 ]; then
    if grep -qF 'Code:' <<< "$out" && ! grep -qF allow_experimental_iceberg_compaction <<< "$out"; then
        echo "FAIL: expected OPTIMIZE to succeed or to name the setting: $out"
    else
        echo "ok"
    fi
elif grep -qF NOT_IMPLEMENTED <<< "$out" \
    && grep -qF 'not yet supported for Iceberg data compaction' <<< "$out"; then
    echo "ok"
else
    echo "FAIL: expected a NOT_IMPLEMENTED refusal on the open-source build: $out"
fi

# Without the setting the error names it (on Cloud, unless the table-level setting is on).
out=$(${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${TABLE}" 2>&1)
if grep -qF allow_experimental_iceberg_compaction <<< "$out" \
    || { [ "$is_cloud" = 1 ] && ! grep -qF 'Code:' <<< "$out"; }; then
    echo "ok"
else
    echo "FAIL: expected the setting gate to report the setting: $out"
fi

# With the table-level setting enabled, Cloud must compact without the query-level one,
# and the open-source build still refuses with both.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${ENABLED_TABLE} (c0 Int32)
    ENGINE = IcebergLocal('${ENABLED_TABLE_PATH}', 'Parquet')
    SETTINGS allow_experimental_iceberg_compaction = 1
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${ENABLED_TABLE} VALUES (1)"

if [ "$is_cloud" = 1 ]; then
    out=$(${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${ENABLED_TABLE}" 2>&1)
else
    out=$(${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 \
        --query "OPTIMIZE TABLE ${ENABLED_TABLE}" 2>&1)
fi

if grep -qF 'Logical error' <<< "$out"; then
    echo "FAIL: logical error: $out"
elif [ "$is_cloud" = 1 ]; then
    if grep -qF 'Code:' <<< "$out"; then
        echo "FAIL: expected OPTIMIZE to succeed on a table with compaction enabled: $out"
    else
        echo "ok"
    fi
elif grep -qF NOT_IMPLEMENTED <<< "$out" \
    && grep -qF 'not yet supported for Iceberg data compaction' <<< "$out"; then
    echo "ok"
else
    echo "FAIL: expected a NOT_IMPLEMENTED refusal on the open-source build: $out"
fi

# The tables are still readable and the server is alive.
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE}"
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${ENABLED_TABLE}"
${CLICKHOUSE_CLIENT} --query "SELECT 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${ENABLED_TABLE} SYNC"
