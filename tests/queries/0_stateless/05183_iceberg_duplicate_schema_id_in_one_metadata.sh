#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: IcebergLocal is non-replicated.

# One table metadata file binding the same schema-id to two different schemas is malformed: nothing
# can resolve which of them the data files were written under. Such metadata must be rejected with
# a clean ICEBERG_SPECIFICATION_VIOLATION, while repeating an identical schema stays harmless.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
rm -rf "${TABLE_PATH}" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} VALUES (1, 'a')"

publish_duplicate_schema() {
    python3 - "${TABLE_PATH}metadata" "$1" <<'PY'
import copy, json, os, re, sys

metadata_dir, mode = sys.argv[1], sys.argv[2]

latest_file = max(
    (f for f in os.listdir(metadata_dir) if re.fullmatch(r"v\d+\.metadata\.json", f)),
    key=lambda f: int(re.match(r"v(\d+)", f).group(1)))

with open(os.path.join(metadata_dir, latest_file)) as fh:
    metadata = json.load(fh)

current = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
duplicate = copy.deepcopy(current)
if mode == "different":
    duplicate["fields"][0]["name"] = "c9"
metadata["schemas"].append(duplicate)
metadata["last-updated-ms"] = metadata.get("last-updated-ms", 0) + 60000

version = int(re.match(r"v(\d+)", latest_file).group(1)) + 1
tmp_file = os.path.join(metadata_dir, ".tmp_next")
with open(tmp_file, "w") as fh:
    json.dump(metadata, fh)
os.rename(tmp_file, os.path.join(metadata_dir, f"v{version}.metadata.json"))
PY
}

# The same schema-id repeated with an identical schema is not a conflict.
publish_duplicate_schema identical
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT count() FROM ${TABLE}"

# The same schema-id bound to two different schemas is rejected.
publish_duplicate_schema different
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT count() FROM ${TABLE}" 2>&1 \
    | grep -q -F "ICEBERG_SPECIFICATION_VIOLATION" && echo "rejected" || echo "NOT REJECTED"

${CLICKHOUSE_CLIENT} --query "SELECT 'alive'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}" 2>/dev/null
