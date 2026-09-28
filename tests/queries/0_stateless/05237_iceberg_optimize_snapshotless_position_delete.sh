#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# A table whose `current-snapshot-id` is `null`, `-1` or absent has no current snapshot and reads as
# empty. `OPTIMIZE` must keep it empty instead of bringing its historical rows back. A zero row count
# alone cannot tell a skipped table from one with nothing to rewrite, so `OPTIMIZE` must also succeed
# and log that it skipped the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

for VARIANT in null negative absent; do
    TABLE="t_${CLICKHOUSE_DATABASE}_${VARIANT}_${RANDOM}"
    TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${TABLE} (a Int32, v Int32)
        ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
        PARTITION BY (a)
        SETTINGS allow_experimental_iceberg_compaction = 1
    "

    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 \
        --query "INSERT INTO ${TABLE} VALUES (1, 1), (1, 2), (1, 3)"
    # A historical position delete is what makes compaction consider the table worth rewriting.
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --use_iceberg_metadata_files_cache=0 \
        --query "ALTER TABLE ${TABLE} DELETE WHERE v = 2"
    # Append history after the delete, so a rewrite has a chain to rebuild from.
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 \
        --query "INSERT INTO ${TABLE} VALUES (1, 4)"

    LATEST_METADATA=$(ls "${TABLE_PATH}"metadata/v*.metadata.json | sed 's#.*/v##;s#\.metadata.json##' | sort -n | tail -1)
    python3 - "${TABLE_PATH}metadata/v${LATEST_METADATA}.metadata.json" "${VARIANT}" <<'PY'
import json, sys
path, variant = sys.argv[1], sys.argv[2]
meta = json.load(open(path))
assert meta.get("current-snapshot-id") not in (None, -1), "expected a live current snapshot to remove"
if variant == "null":
    meta["current-snapshot-id"] = None
elif variant == "negative":
    meta["current-snapshot-id"] = -1
else:
    meta.pop("current-snapshot-id", None)
json.dump(meta, open(path, "w"))
PY

    ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "DETACH TABLE ${TABLE}"
    ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --send_logs_level=fatal --query "ATTACH TABLE ${TABLE}"

    before=$(${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT count() FROM ${TABLE}")
    # The skip is logged at `information`.
    ERR=$(${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --allow_experimental_iceberg_compaction=1 \
        --send_logs_level=information --query "OPTIMIZE TABLE ${TABLE}" 2>&1)
    STATUS=$?
    after=$(${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT count() FROM ${TABLE}")
    if [[ "${STATUS}" -ne 0 ]]; then
        echo "${VARIANT} FAIL: OPTIMIZE failed: ${ERR}"
    elif [[ "${ERR}" != *"No current snapshot found, skipping compaction"* ]]; then
        echo "${VARIANT} FAIL: OPTIMIZE did not skip the table, before=${before} after=${after}: ${ERR}"
    else
        echo "${VARIANT} before=${before} after=${after}"
    fi

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
    rm -rf "${TABLE_PATH}"
done
