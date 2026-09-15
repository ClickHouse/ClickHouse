#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# A table whose `current-snapshot-id` says "no current snapshot" reads as empty. Plain `OPTIMIZE`
# must not bring its historical rows back. It used to: `getHistory` marks no record as a current
# ancestor when there is no current snapshot, but compaction never looked at that - `getPlan` sets
# `need_optimize` from any historical position delete, and the rewrite republished a snapshot chain
# built from append history, restoring the old `current-snapshot-id` and every row with it.
#
# The three spellings of "no current snapshot" are all exercised, because the defect is in the
# compaction path and is indifferent to how the absence is encoded - `null` is merely the spelling
# this pull request taught the readers to accept.
#
# The row counts are asserted on both builds - a cloud build gates `OPTIMIZE` on
# `IcebergCompactionMetadataGenerator` and throws instead of compacting, which leaves the table empty
# for a different reason but never resurrects rows. So that a count of zero cannot pass for the wrong
# reason, the open-source build is additionally held to `OPTIMIZE` succeeding: without that, any
# exception raised before the guard would leave the table empty and the test green. Classify the
# outcome the way the sibling `04846_iceberg_optimize_null_current_snapshot_id` does.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

for VARIANT in null negative absent; do
    TABLE="t_${CLICKHOUSE_DATABASE}_${VARIANT}_${RANDOM}"
    TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${TABLE} (a Int32, v Int32)
        ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
        PARTITION BY (a)
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
    # The exit status, not the presence of output, is what says whether `OPTIMIZE` failed: server
    # logs reach the client's stderr at the harness' log level, so an unrelated warning must not
    # count as a failure. `--send_logs_level=fatal` keeps a real exception's report short.
    ERR=$(${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --allow_experimental_iceberg_compaction=1 \
        --send_logs_level=fatal --query "OPTIMIZE TABLE ${TABLE}" 2>&1)
    STATUS=$?
    after=$(${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT count() FROM ${TABLE}")
    if [[ "${STATUS}" -ne 0 && "${IS_CLOUD}" != "1" ]]; then
        # A quiet no-op is what the guard produces on the open-source build. Without this, an
        # exception raised before the guard would leave the table empty and the count assertion green.
        echo "${VARIANT} FAIL: OPTIMIZE failed on the open-source build: ${ERR}"
    else
        echo "${VARIANT} before=${before} after=${after}"
    fi

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
    rm -rf "${TABLE_PATH}"
done
