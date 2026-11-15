#!/usr/bin/env bash
# `OPTIMIZE TABLE` must preserve and advance an existing `metadata/version-hint.text`.
#
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id Int64)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    SETTINGS iceberg_format_version = 2,
             allow_experimental_iceberg_compaction = 1,
             iceberg_compaction_delay_bias = 86400
"

VERSION_HINT="${TABLE_PATH}metadata/version-hint.text"
printf '1' > "${VERSION_HINT}" || exit 1

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "INSERT INTO ${TABLE} VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query \
    "ALTER TABLE ${TABLE} DELETE WHERE id = 1"

${CLICKHOUSE_CLIENT} --query "SELECT arraySort(groupArray(id)) FROM ${TABLE}"
test -f "${TABLE_PATH}metadata/v3.metadata.json"; echo $?

${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}" >/dev/null || exit 1

# Cloud can return from `OPTIMIZE TABLE` before the new metadata and hint become visible.
for ((attempt = 0; attempt < 30; ++attempt)); do
    if test -f "${TABLE_PATH}metadata/v4.metadata.json" && test -f "${VERSION_HINT}" && test "$(<"${VERSION_HINT}")" = 4; then
        break
    fi
    sleep 1
done

test -f "${VERSION_HINT}"; echo $?
printf '%s\n' "$(<"${VERSION_HINT}")"
test -f "${TABLE_PATH}metadata/v4.metadata.json"; echo $?

${CLICKHOUSE_CLIENT} --query "SELECT arraySort(groupArray(id)) FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
rm -rf "${TABLE_PATH}" 2>/dev/null
