#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# Kept apart from `04846_iceberg_null_current_snapshot_id` because the `OPTIMIZE` commands are the
# build-dependent ones: `IcebergMetadata::optimize` reaches `getHistory` - one of the readers this
# fix normalizes - only when `CLICKHOUSE_CLOUD` is off, and a cloud build gates the command on
# `IcebergCompactionMetadataGenerator` instead. The sibling test covers the readers that behave the
# same in both builds. Rather than skip a build, the assertions below are stated as the invariant
# this fix actually establishes, so they hold on both (see `check_no_conversion_error`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup() {
    # An early exit between DETACH and ATTACH leaves the table detached, where DROP cannot see it.
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE IF NOT EXISTS ${TABLE}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC" 2>/dev/null
    rm -rf "${TABLE_PATH}" 2>/dev/null
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int32, v Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"

INSERTS=$(for i in $(seq 0 4); do echo "INSERT INTO ${TABLE} VALUES (1, ${i});"; done)
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 -m --query "${INSERTS}"

# The Iceberg spec lets `current-snapshot-id` be JSON null to mean "no current snapshot"; external
# writers do emit that. `Poco::JSON::Object::has` is true for a null value, so a reader that only
# checks `has` before `getValue<Int64>` hits a Poco conversion error instead of the no-snapshot path.
LATEST_METADATA=$(ls "${TABLE_PATH}"metadata/v*.metadata.json | sed 's#.*/v##;s#\.metadata.json##' | sort -n | tail -1)
python3 - "${TABLE_PATH}metadata/v${LATEST_METADATA}.metadata.json" <<'PY'
import json, sys
path = sys.argv[1]
meta = json.load(open(path))
assert meta["current-snapshot-id"] is not None, "expected a live current snapshot to null out"
meta["current-snapshot-id"] = None
json.dump(meta, open(path, "w"))
PY

${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --send_logs_level=fatal --query "ATTACH TABLE ${TABLE}"

# What this pull request guarantees is that a JSON-null `current-snapshot-id` never reaches
# `getValue<Int64>`, i.e. never produces `Invalid access: Can not convert empty value`. The exit code
# alone cannot express that, because the two builds legitimately differ in how far `OPTIMIZE` gets:
# the open-source build runs the synchronous path this fix changes, while a cloud build gates
# `OPTIMIZE` on `IcebergCompactionMetadataGenerator`, which the background scheduler creates lazily.
# The table here is freshly re-attached, so on a cloud build that generator does not exist yet and
# the command raises `Logical error: Background compaction is not initialized`, which the test
# harness reports as a failure of whatever job is sharing that server - this is what got the first
# attempt at this change reverted. The command is therefore not run at all on a cloud build, rather
# than run and have its outcome excused, and everything printed below holds on both builds.
IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

# Prints nothing when the command behaves: the reference is made of build-independent lines only.
# The exit status, not the presence of output, is what says whether the command failed - server logs
# reach the client's stderr at the harness' log level, so an unrelated warning must not read as a
# failure - and `--send_logs_level=fatal` keeps a real exception's report short.
run_optimize()
{
    local label=$1 && shift
    local query=$1 && shift
    local err
    local status

    err=$(${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --allow_experimental_iceberg_compaction=1 \
        --send_logs_level=fatal --query "${query}" 2>&1)
    status=$?

    if printf '%s' "${err}" | grep -qF 'Can not convert empty value'; then
        # The regression: `has` was true for the JSON null and `getValue<Int64>` threw.
        echo "FAIL: ${label} hit the JSON-null conversion error"
    elif [[ "${status}" -ne 0 ]]; then
        echo "FAIL: ${label} failed: ${err}"
    fi
}

if [[ "${IS_CLOUD}" != "1" ]]; then
    # `OPTIMIZE TABLE` walks the snapshot ancestry through `IcebergMetadata::getHistory`.
    run_optimize "OPTIMIZE" "OPTIMIZE TABLE ${TABLE}"

    # `OPTIMIZE TABLE ... MANIFEST` takes a different route - `IcebergMetadata::optimizeManifestFiles` ->
    # `compactIcebergManifests` -> `isCurrentManifestListAboveThreshold` - which reads
    # `current-snapshot-id` with its own `has` check.
    run_optimize "OPTIMIZE MANIFEST" "OPTIMIZE TABLE ${TABLE} MANIFEST"
fi

# The table is still readable, still empty.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT count() FROM ${TABLE}"
