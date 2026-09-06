#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: IcebergLocal is non-replicated.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/100304
# Snapshot `schema-id` is optional. ClickHouse falls back to the current table schema.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

latest_metadata_file()
{
    local metadata_path=$1

    for metadata_file in "${metadata_path}"/v*.metadata.json
    do
        basename "${metadata_file}"
    done | sort -t v -k2 -n | tail -1
}

rewrite_metadata()
{
    local metadata_path=$1
    local latest=$2
    local mode=$3

    python3 - "${metadata_path}" "${latest}" "${mode}" <<'PY'
import json
import os
import re
import sys

metadata_path, latest, mode = sys.argv[1:]
with open(os.path.join(metadata_path, latest)) as metadata_file:
    metadata = json.load(metadata_file)

current_snapshot_id = metadata["current-snapshot-id"]
if mode == "historical":
    snapshot = next(snapshot for snapshot in metadata["snapshots"] if snapshot["snapshot-id"] != current_snapshot_id)
else:
    snapshot = next(snapshot for snapshot in metadata["snapshots"] if snapshot["snapshot-id"] == current_snapshot_id)

if mode == "null":
    snapshot["schema-id"] = None
else:
    snapshot.pop("schema-id", None)

metadata["last-updated-ms"] = metadata.get("last-updated-ms", 0) + 60000
version = int(re.match(r"v(\d+)\.metadata\.json", latest).group(1)) + 1
temporary = os.path.join(metadata_path, ".tmp_next")
with open(temporary, "w") as metadata_file:
    json.dump(metadata, metadata_file)
os.rename(temporary, os.path.join(metadata_path, f"v{version}.metadata.json"))
if mode == "historical":
    print(snapshot["snapshot-id"])
PY
}

TABLE1="t1_${CLICKHOUSE_DATABASE}_${RANDOM}"
PATH1="${USER_FILES_PATH}/${TABLE1}/"
rm -rf "${PATH1}" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE1} (c Int32) ENGINE = IcebergLocal('${PATH1}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE1} VALUES (1)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE1}"

LATEST1=$(latest_metadata_file "${PATH1}metadata")
rewrite_metadata "${PATH1}metadata" "${LATEST1}" absent
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT count() FROM ${TABLE1}"

LATEST1=$(latest_metadata_file "${PATH1}metadata")
rewrite_metadata "${PATH1}metadata" "${LATEST1}" null
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT count() FROM ${TABLE1}"

TABLE2="t2_${CLICKHOUSE_DATABASE}_${RANDOM}"
PATH2="${USER_FILES_PATH}/${TABLE2}/"
rm -rf "${PATH2}" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE2} (c Int32) ENGINE = IcebergLocal('${PATH2}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE2} VALUES (1)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE2} VALUES (2)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE2}"

LATEST2=$(latest_metadata_file "${PATH2}metadata")
HISTORICAL_SNAPSHOT_ID=$(rewrite_metadata "${PATH2}metadata" "${LATEST2}" historical)

${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT count() FROM ${TABLE2}"
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --iceberg_snapshot_id="${HISTORICAL_SNAPSHOT_ID}" \
    --query "SELECT count() FROM ${TABLE2}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE1}, ${TABLE2}"
rm -rf "${PATH1}" "${PATH2}" 2>/dev/null

echo "OK"
