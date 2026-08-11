#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: IcebergLocal is non-replicated.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/114350
# An Iceberg schema field with an empty name used to reach ASTIdentifier, whose
# constructor asserts every part is non-empty, so SELECT * aborted the server on
# debug and sanitizer builds. It must now be rejected with a clean
# ICEBERG_SPECIFICATION_VIOLATION, leaving the server up.
#
# Each malformed table is read twice. The schema processor is per table and
# outlives a query, so a rejection that happens after a schema cache is written
# leaves the processor half populated and the second read aborts on the paired
# cache assertion instead of reporting the same error again.

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

echo "OK"
