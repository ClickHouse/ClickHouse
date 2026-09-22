#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# A table whose `current-snapshot-id` says "no current snapshot" reads as empty, and plain
# `OPTIMIZE` must not bring its historical rows back. Four spellings of that state are exercised:
# `current-snapshot-id` set to `-1` or absent, and - since `snapshots` is itself optional - an
# empty and an absent snapshot set. JSON `null` fails earlier, inside the metadata readers, so it
# is out of scope here.
#
# The row counts are asserted on both builds - a cloud build gates `OPTIMIZE` on
# `IcebergCompactionMetadataGenerator` and throws instead of compacting, which leaves the table
# empty for a different reason but never resurrects rows. So that a count of zero cannot pass for
# the wrong reason, the open-source build is additionally held to `OPTIMIZE` succeeding: without
# that, any exception raised before the refusal would leave the table empty and the test green.
# On a cloud build the one thing that must never be reached is the background-compaction
# assertion, which a release build reports as an ordinary exception, so its message is rejected
# regardless of the build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

for VARIANT in negative absent empty_snapshots no_snapshots; do
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
if variant == "negative":
    meta["current-snapshot-id"] = -1
else:
    meta.pop("current-snapshot-id", None)
if variant in ("empty_snapshots", "no_snapshots"):
    assert meta.get("snapshots"), "expected a non-empty snapshot set to remove"
    if variant == "empty_snapshots":
        meta["snapshots"] = []
    else:
        meta.pop("snapshots", None)
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
    if [[ "${ERR}" == *"Background compaction is not initialized"* ]]; then
        echo "${VARIANT} FAIL: OPTIMIZE reached the background-compaction assertion: ${ERR}"
    elif [[ "${STATUS}" -ne 0 && "${IS_CLOUD}" != "1" ]]; then
        echo "${VARIANT} FAIL: OPTIMIZE failed on the open-source build: ${ERR}"
    else
        echo "${VARIANT} before=${before} after=${after}"
    fi

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
    rm -rf "${TABLE_PATH}"
done
