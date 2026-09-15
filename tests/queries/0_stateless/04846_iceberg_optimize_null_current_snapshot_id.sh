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
# the open-source build runs the synchronous path and must end in a quiet no-op, while a cloud build
# gates `OPTIMIZE` on `IcebergCompactionMetadataGenerator`, which the background scheduler creates
# lazily, and reports a user-facing exception instead. Classify the outcome the way
# `04513_iceberg_optimize_orc_position_delete_88123` does, so the conversion error fails everywhere
# and only the OSS build is held to the no-op.
IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

check_no_conversion_error()
{
    local label=$1 && shift
    local err=$1 && shift

    if printf '%s' "${err}" | grep -qF 'Can not convert empty value'; then
        # The regression: `has` was true for the JSON null and `getValue<Int64>` threw.
        echo "FAIL: ${label} hit the JSON-null conversion error"
    elif [[ "${IS_CLOUD}" = "1" ]]; then
        # Cloud gates this command elsewhere; any other exception is not this fix's business.
        echo "${label}: no conversion error"
    elif [[ -n "${err}" ]]; then
        echo "FAIL: ${label} failed on the open-source build: ${err}"
    else
        echo "${label}: no conversion error"
    fi
}

# `OPTIMIZE TABLE` walks the snapshot ancestry through `IcebergMetadata::getHistory`.
check_no_conversion_error "OPTIMIZE" "$(${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 \
    --allow_experimental_iceberg_compaction=1 \
    --query "OPTIMIZE TABLE ${TABLE}" 2>&1)"

# `OPTIMIZE TABLE ... MANIFEST` takes a different route - `IcebergMetadata::optimizeManifestFiles` ->
# `compactIcebergManifests` -> `isCurrentManifestListAboveThreshold` - which reads
# `current-snapshot-id` with its own `has` check.
check_no_conversion_error "OPTIMIZE MANIFEST" "$(${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 \
    --allow_experimental_iceberg_compaction=1 \
    --query "OPTIMIZE TABLE ${TABLE} MANIFEST" 2>&1)"

# The table is still readable, still empty.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT count() FROM ${TABLE}"
