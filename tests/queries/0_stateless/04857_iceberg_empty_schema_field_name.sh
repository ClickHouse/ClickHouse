#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: IcebergLocal is non-replicated.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/114350
# An Iceberg schema field with an empty name used to reach ASTIdentifier, whose
# constructor asserts every part is non-empty, so SELECT * aborted the server on
# debug and sanitizer builds. It must now be rejected with a clean
# ICEBERG_SPECIFICATION_VIOLATION, leaving the server up.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
rm -rf "${TABLE_PATH}" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int64, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} VALUES (1, 'a')"

# Control: a well formed lake reads normally.
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${TABLE}"

# Publish a newer metadata version adding a NEW schema-id whose second field has
# an empty name, and make it current. The malformed schema must be a new id:
# editing schema 0 in place desynchronizes the Avro manifest and fails earlier
# with a different error, masking this defect.
LATEST=$(ls "${TABLE_PATH}metadata/" | grep -E '^v[0-9]+\.metadata\.json$' | sort -t v -k2 -n | tail -1)
python3 - "${TABLE_PATH}metadata" "${LATEST}" <<'PY'
import copy, json, os, re, sys

metadata_dir, latest_file = sys.argv[1], sys.argv[2]

with open(os.path.join(metadata_dir, latest_file)) as fh:
    metadata = json.load(fh)

current = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
malformed = copy.deepcopy(current)
malformed["schema-id"] = max(s["schema-id"] for s in metadata["schemas"]) + 1
malformed["fields"][1]["name"] = ""

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

${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT * FROM ${TABLE} FORMAT Null" 2>&1 \
    | grep -q -F "ICEBERG_SPECIFICATION_VIOLATION" && echo "rejected" || echo "NOT REJECTED"

# The server must still be alive (no abort).
${CLICKHOUSE_CLIENT} --query "SELECT 'alive'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}" 2>/dev/null

echo "OK"
