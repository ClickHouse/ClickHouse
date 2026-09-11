#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
NULL_TABLE="tnull_${CLICKHOUSE_DATABASE}_${RANDOM}"
NULL_TABLE_PATH="${USER_FILES_PATH}/${NULL_TABLE}/"

# Drop the tables before removing their directories, so an early exit cannot leave an attached
# table pointing at a path that no longer exists.
cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${NULL_TABLE} SYNC" 2>/dev/null
    rm -rf "${TABLE_PATH}" "${NULL_TABLE_PATH}" 2>/dev/null
}
trap cleanup EXIT

# An Iceberg table with enough manifests for `OPTIMIZE ... MANIFEST` to have real work to do.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int32, v Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"

INSERTS=$(for i in $(seq 0 34); do echo "INSERT INTO ${TABLE} VALUES (1, ${i});"; done)
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 -m --query "${INSERTS}"

# Make the metadata self-contradictory the way a mis-pruned `snapshots` list does: keep a live
# `current-snapshot-id` but drop that snapshot from `snapshots`. Inserts and mutations already
# reject this; manifest compaction used to read it as "no current snapshot" and report success,
# so `OPTIMIZE TABLE ... MANIFEST` silently did nothing on corrupt metadata.
LATEST_METADATA=$(ls "${TABLE_PATH}"metadata/v*.metadata.json | sed 's#.*/v##;s#\.metadata.json##' | sort -n | tail -1)
python3 - "${TABLE_PATH}metadata/v${LATEST_METADATA}.metadata.json" <<'PY'
import json, sys
path = sys.argv[1]
meta = json.load(open(path))
current = meta["current-snapshot-id"]
assert current is not None and current >= 0, "expected a live current snapshot to remove"
meta["snapshots"] = [s for s in meta["snapshots"] if s["snapshot-id"] != current]
json.dump(meta, open(path, "w"))
PY

# Drop the in-memory metadata so the edited file is re-read.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --allow_repeated_settings --send_logs_level=fatal --query "ATTACH TABLE ${TABLE}"

# Must fail loudly instead of returning success without consolidating anything.
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --use_iceberg_metadata_files_cache=0 \
    --query "OPTIMIZE TABLE ${TABLE} MANIFEST SETTINGS iceberg_manifest_min_count_to_compact=5" 2>&1 \
    | grep -oF 'ICEBERG_SPECIFICATION_VIOLATION' | head -n1

# A `current-snapshot-id` of JSON null is the spec's way of saying "no current snapshot" and must stay
# a quiet no-op, not an error: `Poco::JSON::Object::has` is true for a null value, so reading it with
# `getValue<Int64>` would throw an unrelated conversion exception instead.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${NULL_TABLE} (a Int32, v Int32)
    ENGINE = IcebergLocal('${NULL_TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"
NULL_INSERTS=$(for i in $(seq 0 5); do echo "INSERT INTO ${NULL_TABLE} VALUES (1, ${i});"; done)
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 -m --query "${NULL_INSERTS}"

NULL_LATEST=$(ls "${NULL_TABLE_PATH}"metadata/v*.metadata.json | sed 's#.*/v##;s#\.metadata.json##' | sort -n | tail -1)
python3 - "${NULL_TABLE_PATH}metadata/v${NULL_LATEST}.metadata.json" <<'PY'
import json, sys
path = sys.argv[1]
meta = json.load(open(path))
meta["current-snapshot-id"] = None
json.dump(meta, open(path, "w"))
PY

${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "DETACH TABLE ${NULL_TABLE}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --allow_repeated_settings --send_logs_level=fatal --query "ATTACH TABLE ${NULL_TABLE}"

# Assert the exit status as well as the output: grepping alone would print 0 for a client that failed
# with a message that merely does not contain the word "Exception".
NULL_OUTPUT=$(${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --use_iceberg_metadata_files_cache=0 \
    --query "OPTIMIZE TABLE ${NULL_TABLE} MANIFEST SETTINGS iceberg_manifest_min_count_to_compact=5" 2>&1)
NULL_STATUS=$?
echo "${NULL_STATUS} $(printf '%s' "${NULL_OUTPUT}" | grep -cF 'Exception')"

# The server is still alive.
${CLICKHOUSE_CLIENT} --query "SELECT 1"
