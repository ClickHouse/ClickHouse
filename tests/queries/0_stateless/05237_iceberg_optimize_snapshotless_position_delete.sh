#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# A table whose `current-snapshot-id` says "no current snapshot" reads as empty, and plain
# `OPTIMIZE` must not bring its historical rows back. Five spellings of that state are exercised:
# `current-snapshot-id` set to `null`, to `-1`, or absent, and - since `snapshots` is itself
# optional - an empty and an absent snapshot set. A sixth case pairs that state with an external
# `format-version` upgrade (the Spark v1 -> v2 case `PersistentTableComponents` documents): the
# table stays open across the upgrade, so its cached open-time version no longer matches the
# metadata file's, a state `OPTIMIZE` must also survive.
#
# The row counts are asserted on both builds, and both `CREATE`s enable compaction per table so a
# cloud build reaches the guard instead of refusing before it. A count of zero alone cannot pass for
# the right reason - an exception before the refusal, or a table with nothing to rewrite, also
# leaves it empty - so `OPTIMIZE` is additionally held to succeeding and to logging the refusal it
# is supposed to take. That log statement sits above the `#if CLICKHOUSE_CLOUD` split, so the
# assertion holds on either build. The background-compaction assertion must never be reached, and a
# release build reports it as an ordinary exception, so its message is rejected on every build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

for VARIANT in null negative absent empty_snapshots no_snapshots; do
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
    # The exit status, not the presence of output, is what says whether `OPTIMIZE` failed, so an
    # unrelated log line must not count as a failure. The level is raised to `information` for this
    # one call because the refusal under test announces itself there: a row count cannot tell "the
    # guard refused" from "there was nothing to rewrite".
    ERR=$(${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --allow_experimental_iceberg_compaction=1 \
        --send_logs_level=information --query "OPTIMIZE TABLE ${TABLE}" 2>&1)
    STATUS=$?
    after=$(${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT count() FROM ${TABLE}")
    if [[ "${ERR}" == *"Background compaction is not initialized"* ]]; then
        echo "${VARIANT} FAIL: OPTIMIZE reached the background-compaction assertion: ${ERR}"
    elif [[ "${STATUS}" -ne 0 ]]; then
        echo "${VARIANT} FAIL: OPTIMIZE failed: ${ERR}"
    elif [[ "${ERR}" != *"No snapshot is a current ancestor"* ]]; then
        echo "${VARIANT} FAIL: OPTIMIZE did not reach the snapshotless guard: ${ERR}"
    else
        echo "${VARIANT} before=${before} after=${after}"
    fi

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
    rm -rf "${TABLE_PATH}"
done

# The upgrade case cannot reuse the loop: `persistent_components.format_version` is the value read
# when the table was opened, so the `DETACH`/`ATTACH` above would re-read the upgraded file and the
# mismatch could not exist, and a v1 table refuses the loop's position delete. The metadata files
# cache is off from the `CREATE` on, so the edit below is the state `OPTIMIZE` actually reads.
# Server logs are silenced throughout: reading a v1 table warns that its schema parses only under
# the v2 method, which is how ClickHouse writes v1 metadata and is unrelated to what is tested.
FMT_CLIENT="${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --send_logs_level=fatal"
TABLE="t_${CLICKHOUSE_DATABASE}_format_upgrade_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${FMT_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int32, v Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
    SETTINGS iceberg_format_version = 1, allow_experimental_iceberg_compaction = 1
"

${FMT_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 1), (1, 2), (1, 3)"
${FMT_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 4)"
# Reading here opens the table, which is what pins the cached version at 1 before the upgrade.
${FMT_CLIENT} --query "SELECT count() FROM ${TABLE}" > /dev/null

LATEST_METADATA=$(ls "${TABLE_PATH}"metadata/v*.metadata.json | sed 's#.*/v##;s#\.metadata.json##' | sort -n | tail -1)
python3 - "${TABLE_PATH}metadata/v${LATEST_METADATA}.metadata.json" <<'PY'
import json, sys
path = sys.argv[1]
meta = json.load(open(path))
assert meta.get("format-version") == 1, "expected a v1 table to upgrade"
assert meta.get("current-snapshot-id") not in (None, -1), "expected a live current snapshot to remove"
meta["format-version"] = 2
meta.pop("current-snapshot-id", None)
json.dump(meta, open(path, "w"))
PY

before=$(${FMT_CLIENT} --query "SELECT count() FROM ${TABLE}")
ERR=$(${FMT_CLIENT} --allow_experimental_iceberg_compaction=1 --send_logs_level=information \
    --query "OPTIMIZE TABLE ${TABLE}" 2>&1)
STATUS=$?
after=$(${FMT_CLIENT} --query "SELECT count() FROM ${TABLE}")
if [[ "${ERR}" == *"Background compaction is not initialized"* ]]; then
    echo "format_upgrade FAIL: OPTIMIZE reached the background-compaction assertion: ${ERR}"
elif [[ "${STATUS}" -ne 0 ]]; then
    echo "format_upgrade FAIL: OPTIMIZE failed: ${ERR}"
elif [[ "${ERR}" != *"No snapshot is a current ancestor"* ]]; then
    echo "format_upgrade FAIL: OPTIMIZE did not reach the snapshotless guard: ${ERR}"
else
    echo "format_upgrade before=${before} after=${after}"
fi

${FMT_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
rm -rf "${TABLE_PATH}"
