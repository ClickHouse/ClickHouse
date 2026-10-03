#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

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
# writers do emit that. `Poco::JSON::Object::has` is true for a null value, so readers that only
# check `has` before `getValue<Int64>` hit a Poco conversion error instead of the no-snapshot path.
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

# Each command below reaches a different unguarded read of `current-snapshot-id`. Print the client
# exit status next to the exception count: grepping alone would report 0 for a client that failed
# with a message not containing the word "Exception".
run_and_report() {
    local output
    output=$("$@" 2>&1)
    local status=$?
    echo "${status} $(printf '%s' "${output}" | grep -cF 'Exception')"
}

# IcebergMetadata::getHistory
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 \
    --query "SELECT count() FROM system.iceberg_history WHERE table = '${TABLE}'"

# ExpireSnapshotsExecute
run_and_report ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 \
    --allow_insert_into_iceberg=1 --allow_experimental_expire_snapshots=1 \
    --query "ALTER TABLE ${TABLE} EXECUTE expire_snapshots('2030-01-01 00:00:00')"

# Mutations: both the parent-snapshot lookup and the table-state snapshot
run_and_report ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 \
    --allow_insert_into_iceberg=1 --query "DELETE FROM ${TABLE} WHERE v = 1"

# A table whose current snapshot is null reads as empty rather than erroring.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT count() FROM ${TABLE}"
