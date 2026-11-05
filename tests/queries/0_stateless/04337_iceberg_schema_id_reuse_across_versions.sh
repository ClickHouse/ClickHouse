#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: IcebergLocal is non-replicated.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/107316
# A metadata version that re-binds an existing schema-id to different fields
# used to abort the server with a chassert in addIcebergTableSchema (debug) and
# silently keep the stale cached schema (release). It must now be rejected with
# a clean ICEBERG_SPECIFICATION_VIOLATION in all builds, leaving the server up.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
rm -rf "${TABLE_PATH}" 2>/dev/null

# schema_id 0 = {c0 Int32, c1 String}; seed one row.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} VALUES (1, 'a')"

# Warm read: caches schema_id 0 -> {c0,c1} in the table's persistent schema_processor.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

# Publish a newer metadata version that RE-USES schema_id 0 with different
# content (rename c0 -> c9); current-schema-id stays 0.
LATEST=$(ls "${TABLE_PATH}metadata/" | grep -E '^v[0-9]+\.metadata\.json$' | sort -t v -k2 -n | tail -1)
python3 - "${TABLE_PATH}metadata" "${LATEST}" <<'PY'
import json, os, sys, re

metadata_dir, latest_file = sys.argv[1], sys.argv[2]

with open(os.path.join(metadata_dir, latest_file)) as fh:
    metadata = json.load(fh)

for schema in metadata["schemas"]:
    if schema["schema-id"] == 0:
        for field in schema["fields"]:
            if field["name"] == "c0":
                field["name"] = "c9"

metadata["last-updated-ms"] = metadata.get("last-updated-ms", 0) + 60000

version = int(re.match(r"v(\d+)\.metadata\.json", latest_file).group(1)) + 1
next_file = os.path.join(metadata_dir, f"v{version}.metadata.json")

tmp_file = os.path.join(metadata_dir, ".tmp_next")
with open(tmp_file, "w") as fh:
    json.dump(metadata, fh)
os.rename(tmp_file, next_file)
PY

# Forced fresh re-read re-parses the new version: addIcebergTableSchema(0, {c9,c1})
# collides with the cached {c0,c1}. Expect a clean specification-violation error.
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT count() FROM ${TABLE}" 2>&1 \
    | grep -q -F "ICEBERG_SPECIFICATION_VIOLATION" && echo "rejected" || echo "NOT REJECTED"

# The server must still be alive (no abort).
${CLICKHOUSE_CLIENT} --query "SELECT 'alive'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}" 2>/dev/null

echo "OK"
