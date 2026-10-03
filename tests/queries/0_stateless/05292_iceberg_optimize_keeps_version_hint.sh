#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/120164: `OPTIMIZE TABLE` must not delete `metadata/version-hint.text` and leave the table unreadable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

# `iceberg_use_version_hint = 1` resolves reads through `metadata/version-hint.text`; compaction requires format version 2, pinned explicitly.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id Int64)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    SETTINGS iceberg_format_version = 2, iceberg_use_version_hint = 1
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "INSERT INTO ${TABLE} VALUES (1), (2), (3)"
# A position delete is what makes the compaction path actually run.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query \
    "ALTER TABLE ${TABLE} DELETE WHERE id = 1"

echo "before: $(${CLICKHOUSE_CLIENT} --query "SELECT arraySort(groupArray(id)) FROM ${TABLE}")"

VERSION_HINT="${TABLE_PATH}metadata/version-hint.text"
if [[ ! -f "${VERSION_HINT}" ]]; then
    echo "version-hint.text missing before OPTIMIZE"
    exit 1
fi
hint_before=$(<"${VERSION_HINT}")

# Require `OPTIMIZE` to succeed; readability alone also holds when compaction fails before committing.
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query \
    "OPTIMIZE TABLE ${TABLE}" >/dev/null || exit 1

# The buggy build deletes the pointer here.
if [[ -f "${VERSION_HINT}" ]]; then
    echo "version-hint.text present"
    hint_after=$(<"${VERSION_HINT}")
else
    echo "version-hint.text deleted"
    hint_after=""
fi

# A successful no-op must not pass: the compacted metadata version has to be published.
if [[ "${hint_before}" =~ ^[0-9]+$ && "${hint_after}" =~ ^[0-9]+$ ]] && (( 10#${hint_after} > 10#${hint_before} )); then
    echo "metadata version advanced"
else
    echo "metadata version did not advance"
fi

# Load-bearing assertion: the table stays readable through the hint (buggy build throws FILE_DOESNT_EXIST, captured so only the reference mismatch fails the test).
after=$(${CLICKHOUSE_CLIENT} --query "SELECT arraySort(groupArray(id)) FROM ${TABLE}" 2>&1)
if [[ "${after}" == "[2,3]" ]]; then
    echo "after: readable [2,3]"
else
    echo "after: UNREADABLE"
fi

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
rm -rf "${TABLE_PATH}" 2>/dev/null
