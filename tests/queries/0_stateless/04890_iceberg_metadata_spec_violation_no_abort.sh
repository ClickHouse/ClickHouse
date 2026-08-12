#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: IcebergLocal is non-replicated.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/114487
# Three validators in the Iceberg schema path reject spec-violating metadata
# content. They used to raise LOGICAL_ERROR, which aborts the server in debug and
# sanitizer builds and under abort_on_logical_error. Each must now raise a clean
# ICEBERG_SPECIFICATION_VIOLATION, leaving the server up.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# --- Arm 1: a new field with no counterpart in the old schema is "required" ----

TABLE1="t1_${CLICKHOUSE_DATABASE}_${RANDOM}"
PATH1="${USER_FILES_PATH}/${TABLE1}/"
rm -rf "${PATH1}" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE1} (c0 Int32, c1 String) ENGINE = IcebergLocal('${PATH1}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE1} VALUES (1, 'a')"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE1}"

LATEST1=$(ls "${PATH1}metadata/" | grep -E '^v[0-9]+\.metadata\.json$' | sort -t v -k2 -n | tail -1)
python3 - "${PATH1}metadata" "${LATEST1}" <<'PY'
import copy, json, os, re, sys

metadata_dir, latest_file = sys.argv[1], sys.argv[2]
with open(os.path.join(metadata_dir, latest_file)) as fh:
    metadata = json.load(fh)

schema = copy.deepcopy(metadata["schemas"][0])
schema["schema-id"] = 1
schema["fields"].append({"id": 3, "name": "extra", "required": True, "type": "long"})
metadata["schemas"].append(schema)
metadata["current-schema-id"] = 1
metadata["last-column-id"] = 3
metadata["last-updated-ms"] = metadata.get("last-updated-ms", 0) + 60000

version = int(re.match(r"v(\d+)\.metadata\.json", latest_file).group(1)) + 1
tmp_file = os.path.join(metadata_dir, ".tmp_next")
with open(tmp_file, "w") as fh:
    json.dump(metadata, fh)
os.rename(tmp_file, os.path.join(metadata_dir, f"v{version}.metadata.json"))
PY

${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT * FROM ${TABLE1}" 2>&1 \
    | grep -q -F "ICEBERG_SPECIFICATION_VIOLATION" && echo "required column rejected" || echo "NOT REJECTED"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE1}"
rm -rf "${PATH1}" 2>/dev/null

# --- Arm 2: an old struct-typed field becomes a primitive in the new schema ----

TABLE2="t2_${CLICKHOUSE_DATABASE}_${RANDOM}"
PATH2="${USER_FILES_PATH}/${TABLE2}/"
rm -rf "${PATH2}" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE2} (c0 Int32, s Tuple(a Int32, b String)) ENGINE = IcebergLocal('${PATH2}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE2} VALUES (1, (1, 'b'))"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE2}"

LATEST2=$(ls "${PATH2}metadata/" | grep -E '^v[0-9]+\.metadata\.json$' | sort -t v -k2 -n | tail -1)
python3 - "${PATH2}metadata" "${LATEST2}" <<'PY'
import copy, json, os, re, sys

metadata_dir, latest_file = sys.argv[1], sys.argv[2]
with open(os.path.join(metadata_dir, latest_file)) as fh:
    metadata = json.load(fh)

schema = copy.deepcopy(metadata["schemas"][0])
schema["schema-id"] = 1
for field in schema["fields"]:
    if field["name"] == "s":
        field["type"] = "long"
metadata["schemas"].append(schema)
metadata["current-schema-id"] = 1
metadata["last-updated-ms"] = metadata.get("last-updated-ms", 0) + 60000

version = int(re.match(r"v(\d+)\.metadata\.json", latest_file).group(1)) + 1
tmp_file = os.path.join(metadata_dir, ".tmp_next")
with open(tmp_file, "w") as fh:
    json.dump(metadata, fh)
os.rename(tmp_file, os.path.join(metadata_dir, f"v{version}.metadata.json"))
PY

${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT * FROM ${TABLE2}" 2>&1 \
    | grep -q -F "ICEBERG_SPECIFICATION_VIOLATION" && echo "complex to primitive rejected" || echo "NOT REJECTED"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE2}"
rm -rf "${PATH2}" 2>/dev/null

# --- Arm 3: one snapshot-id bound to two different schema-ids ------------------

TABLE3="t3_${CLICKHOUSE_DATABASE}_${RANDOM}"
PATH3="${USER_FILES_PATH}/${TABLE3}/"
rm -rf "${PATH3}" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE3} (c0 Int32, c1 String) ENGINE = IcebergLocal('${PATH3}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE3} VALUES (1, 'c')"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE3}"

LATEST3=$(ls "${PATH3}metadata/" | grep -E '^v[0-9]+\.metadata\.json$' | sort -t v -k2 -n | tail -1)
python3 - "${PATH3}metadata" "${LATEST3}" <<'PY'
import copy, json, os, re, sys

metadata_dir, latest_file = sys.argv[1], sys.argv[2]
with open(os.path.join(metadata_dir, latest_file)) as fh:
    metadata = json.load(fh)

schema = copy.deepcopy(metadata["schemas"][0])
schema["schema-id"] = 1
metadata["schemas"].append(schema)

snapshot = copy.deepcopy(metadata["snapshots"][0])
snapshot["schema-id"] = 1
metadata["snapshots"].append(snapshot)
metadata["last-updated-ms"] = metadata.get("last-updated-ms", 0) + 60000

version = int(re.match(r"v(\d+)\.metadata\.json", latest_file).group(1)) + 1
tmp_file = os.path.join(metadata_dir, ".tmp_next")
with open(tmp_file, "w") as fh:
    json.dump(metadata, fh)
os.rename(tmp_file, os.path.join(metadata_dir, f"v{version}.metadata.json"))
PY

${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT * FROM ${TABLE3}" 2>&1 \
    | grep -q -F "ICEBERG_SPECIFICATION_VIOLATION" && echo "snapshot schema rebinding rejected" || echo "NOT REJECTED"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE3}"
rm -rf "${PATH3}" 2>/dev/null

# The server must still be alive: none of the three rejections aborted it.
${CLICKHOUSE_CLIENT} --query "SELECT 'alive'"

echo "OK"
