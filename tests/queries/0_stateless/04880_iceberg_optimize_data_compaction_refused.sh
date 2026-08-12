#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Pins the refusal contract of Iceberg data compaction: in the open-source build
# `OPTIMIZE TABLE` on an Iceberg table reports `NOT_IMPLEMENTED` rather than running a
# rewrite it cannot publish. Data compaction is a private feature on Cloud, so only the
# open-source outcome is pinned there; the setting gate is common to both builds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

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
    echo "ok"
elif grep -qF NOT_IMPLEMENTED <<< "$out" \
    && grep -qF 'not yet supported for Iceberg data compaction' <<< "$out"; then
    echo "ok"
else
    echo "FAIL: expected a NOT_IMPLEMENTED refusal on the open-source build: $out"
fi

# The setting gate is unchanged in both builds: without the setting the error names it.
out=$(${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${TABLE}" 2>&1)
if grep -qF allow_experimental_iceberg_compaction <<< "$out"; then
    echo "ok"
else
    echo "FAIL: expected the setting gate to report the setting: $out"
fi

# The table is still readable and the server is alive.
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE}"
${CLICKHOUSE_CLIENT} --query "SELECT 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
