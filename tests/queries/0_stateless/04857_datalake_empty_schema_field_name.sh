#!/usr/bin/env bash
# Tags: no-fasttest, no-msan, no-replicated-database
# Tag no-replicated-database: IcebergLocal is non-replicated.
# Tag no-msan: the delta-kernel-rs library is not built with MSan, so the deltaLakeLocal
# table function is not registered there and the delta case below would fail with
# UNKNOWN_FUNCTION.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/114350
# A lake schema field with an empty name used to reach ASTIdentifier, whose constructor
# asserts every part is non-empty, so SELECT * aborted the server on debug and sanitizer
# builds. It must now be rejected with a clean error, leaving the server up. The defect is
# reachable through Iceberg, both Delta readers and Paimon, so there is one case per lake.
#
# Each malformed Iceberg table is read twice. The schema processor is per table and
# outlives a query, so a rejection that happens after a schema cache is written leaves the
# processor half populated and the second read aborts on the paired cache assertion instead
# of reporting the same error again.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Publish a newer metadata version adding a NEW schema-id with an empty field
# name, and make it current. The malformed schema must be a new id: editing an
# existing one in place desynchronizes the Avro manifest and fails earlier with
# a different error, masking this defect. $1 = metadata dir, $2 = root|nested.
publish_malformed_schema() {
    local metadata_dir="$1"
    local where="$2"
    local latest
    latest=$(ls "${metadata_dir}" | grep -E '^v[0-9]+\.metadata\.json$' | sort -t v -k2 -n | tail -1)

    python3 - "${metadata_dir}" "${latest}" "${where}" <<'PY'
import copy, json, os, re, sys

metadata_dir, latest_file, where = sys.argv[1], sys.argv[2], sys.argv[3]

with open(os.path.join(metadata_dir, latest_file)) as fh:
    metadata = json.load(fh)

current = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
malformed = copy.deepcopy(current)
malformed["schema-id"] = max(s["schema-id"] for s in metadata["schemas"]) + 1
if where == "root":
    malformed["fields"][1]["name"] = ""
else:
    malformed["fields"][1]["type"]["fields"][1]["name"] = ""

metadata["schemas"].append(malformed)
metadata["current-schema-id"] = malformed["schema-id"]
metadata["last-updated-ms"] = metadata.get("last-updated-ms", 0) + 60000

version = int(re.match(r"v(\d+)\.metadata\.json", latest_file).group(1)) + 1
next_file = os.path.join(metadata_dir, f"v{version}.metadata.json")

tmp_file = os.path.join(metadata_dir, ".tmp_next")
with open(tmp_file, "w") as fh:
    json.dump(metadata, fh)
os.rename(tmp_file, next_file)
PY
}

# $1 = table, $2 = expected error text, $3 = token printed on a match.
expect_rejected() {
    ${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT * FROM $1 FORMAT Null" 2>&1 \
        | grep -q -F "$2" && echo "$3" || echo "NOT REJECTED"
}

ROOT_TABLE="t_root_${CLICKHOUSE_DATABASE}_${RANDOM}"
ROOT_PATH="${USER_FILES_PATH}/${ROOT_TABLE}/"
NESTED_TABLE="t_nested_${CLICKHOUSE_DATABASE}_${RANDOM}"
NESTED_PATH="${USER_FILES_PATH}/${NESTED_TABLE}/"
rm -rf "${ROOT_PATH}" "${NESTED_PATH}" 2>/dev/null

# A top-level field name is rejected as a specification violation.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${ROOT_TABLE} (c0 Int64, c1 String) ENGINE = IcebergLocal('${ROOT_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${ROOT_TABLE} VALUES (1, 'a')"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${ROOT_TABLE}"

publish_malformed_schema "${ROOT_PATH}metadata" root
expect_rejected "${ROOT_TABLE}" "ICEBERG_SPECIFICATION_VIOLATION" "rejected"
expect_rejected "${ROOT_TABLE}" "ICEBERG_SPECIFICATION_VIOLATION" "rejected-again"

${CLICKHOUSE_CLIENT} --query "SELECT 'alive'"

# A struct subfield name is rejected one level down, by the tuple type.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${NESTED_TABLE} (c0 Int64, c1 Tuple(a Int64, b String)) ENGINE = IcebergLocal('${NESTED_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${NESTED_TABLE} VALUES (1, (2, 'x'))"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${NESTED_TABLE}"

publish_malformed_schema "${NESTED_PATH}metadata" nested
expect_rejected "${NESTED_TABLE}" "Names of tuple elements cannot be empty" "nested-rejected"
expect_rejected "${NESTED_TABLE}" "Names of tuple elements cannot be empty" "nested-rejected-again"

${CLICKHOUSE_CLIENT} --query "SELECT 'alive'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${ROOT_TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${NESTED_TABLE}"
rm -rf "${ROOT_PATH}" "${NESTED_PATH}" 2>/dev/null

# Reads a query's combined output from stdin. On a match print a stable "<label>: <marker>"
# line, otherwise print what actually happened so a CI failure is diagnosable.
expect_contains() {
    local label="$1" marker="$2" out
    out=$(cat)
    if printf '%s\n' "$out" | grep -qF "$marker"; then
        echo "$label: $marker"
    else
        echo "$label: expected '$marker', got:"
        printf '%s\n' "$out" | head -3
    fi
}

# The lakes below are read through the server, which only opens paths under user_files.
LAKE_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/datalake_empty_name"
rm -rf "${LAKE_DIR}"
mkdir -p "${LAKE_DIR}"
trap 'rm -rf "${LAKE_DIR}"' EXIT

# ClickHouse cannot create a Delta table, so the lake is assembled by hand: a parquet data
# file plus a transaction log whose metaData.schemaString names the second field "".
# $1 = lake dir, $2 = second field name.
stage_delta_lake() {
    python3 - "$1" "$2" "${CLICKHOUSE_LOCAL}" <<'PY'
import json, os, subprocess, sys, uuid
lake, field_name, ch_local = os.path.abspath(sys.argv[1]), sys.argv[2], sys.argv[3]
os.makedirs(os.path.join(lake, "_delta_log"), exist_ok=True)
data = "part-00000-%s-c000.snappy.parquet" % uuid.uuid4()
subprocess.run(
    ch_local.split() + ["-q",
        f"INSERT INTO FUNCTION file('{lake}/{data}', Parquet, 'id Int64, s String') "
        "SELECT number, toString(number) FROM numbers(5) "
        "SETTINGS engine_file_truncate_on_insert=1"],
    check=True, stdin=subprocess.DEVNULL)
schema = {"type": "struct", "fields": [
    {"name": "id", "type": "long", "nullable": True, "metadata": {}},
    {"name": field_name, "type": "string", "nullable": True, "metadata": {}}]}
with open(os.path.join(lake, "_delta_log", "00000000000000000000.json"), "w") as fh:
    for action in [
            {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
            {"metaData": {"id": str(uuid.uuid4()),
                          "format": {"provider": "parquet", "options": {}},
                          "schemaString": json.dumps(schema), "partitionColumns": [],
                          "configuration": {}, "createdTime": 1600000000000}},
            {"add": {"path": data, "partitionValues": {},
                     "size": os.path.getsize(os.path.join(lake, data)),
                     "modificationTime": 1600000000000, "dataChange": True}}]:
        fh.write(json.dumps(action) + "\n")
PY
}

stage_delta_lake "${LAKE_DIR}/dl" ""
stage_delta_lake "${LAKE_DIR}/dl_ok" "s"

# Both Delta readers must reject it. The kernel-rs reader and the C++ reader parse the
# schema separately, so a guard placed in either parser alone would leave the other one
# aborting; allow_experimental_delta_kernel_rs selects between them.
for kernel in 1 0; do
    ${CLICKHOUSE_CLIENT} --allow_experimental_delta_kernel_rs="${kernel}" \
        --query "SELECT * FROM deltaLakeLocal('${LAKE_DIR}/dl') FORMAT Null" 2>&1 \
        | expect_contains "delta_kernel_rs_${kernel}" AMBIGUOUS_COLUMN_NAME
    # A well-named lake still reads, so the check does not reject every Delta table.
    ${CLICKHOUSE_CLIENT} --allow_experimental_delta_kernel_rs="${kernel}" \
        --query "SELECT count() FROM deltaLakeLocal('${LAKE_DIR}/dl_ok')"
done

${CLICKHOUSE_CLIENT} --query "SELECT 'alive'"

# Paimon reads its field names from schema/schema-N. Start from the checked-in table so the
# manifests and snapshot stay consistent, and blank out one field name.
cp -r "${CURDIR}/data_minio/paimon_no_partition" "${LAKE_DIR}/pm"
cp -r "${CURDIR}/data_minio/paimon_no_partition" "${LAKE_DIR}/pm_ok"
python3 - "${LAKE_DIR}/pm/schema/schema-0" <<'PY'
import json, sys
path = sys.argv[1]
with open(path) as fh:
    schema = json.load(fh)
schema["fields"][1]["name"] = ""
with open(path, "w") as fh:
    json.dump(schema, fh)
PY

${CLICKHOUSE_CLIENT} --query "SELECT * FROM paimonLocal('${LAKE_DIR}/pm') FORMAT Null" 2>&1 \
    | expect_contains paimon AMBIGUOUS_COLUMN_NAME
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM paimonLocal('${LAKE_DIR}/pm_ok')"

${CLICKHOUSE_CLIENT} --query "SELECT 'alive'"

echo "OK"
