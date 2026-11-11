#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Iceberg writes.

# The first INSERT after an external upgrade of an Iceberg table from format version 2 to 3 must assign
# row ids to the data written before the upgrade too, and the table's `next-row-id` must cover them.
# https://github.com/ClickHouse/ClickHouse/issues/125334

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_v3_upgrade_row_ids"
rm -rf "${TABLE_PATH}"

# A manifest of EXISTING entries (two inserts merged), a plain insert manifest and a position-delete manifest, all without `first_row_id`.
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    CREATE TABLE t_v3_upgrade (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}');
    INSERT INTO t_v3_upgrade SELECT number FROM numbers(3);
    INSERT INTO t_v3_upgrade SELECT number + 10 FROM numbers(2);
    SET allow_experimental_iceberg_compaction = 1;
    OPTIMIZE TABLE t_v3_upgrade MANIFEST SETTINGS iceberg_manifest_min_count_to_compact = 1;
    INSERT INTO t_v3_upgrade SELECT number + 40 FROM numbers(2);
    ALTER TABLE t_v3_upgrade DELETE WHERE x = 0;
"

# An external engine upgrades the table to format version 3: a new metadata version with `next-row-id`
# initialized to 0 and the existing snapshots left untouched.
python3 - "${TABLE_PATH}/metadata" <<'PY'
import glob, json, os, re, sys
metadata_dir = sys.argv[1]
latest = max(glob.glob(os.path.join(metadata_dir, "v*.metadata.json")),
             key=lambda path: int(re.search(r"/v(\d+)\.metadata\.json$", path).group(1)))
version = int(re.search(r"/v(\d+)\.metadata\.json$", latest).group(1))
with open(latest) as f:
    metadata = json.load(f)
metadata["format-version"] = 3
metadata["next-row-id"] = 0
with open(os.path.join(metadata_dir, f"v{version + 1}.metadata.json"), "w") as f:
    json.dump(metadata, f, indent=4)
PY

print_state()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT x, _row_id, _last_updated_sequence_number FROM icebergLocal('${TABLE_PATH}') ORDER BY x"
    python3 - "${TABLE_PATH}/metadata" <<'PY'
import glob, json, os, re, sys
metadata_dir = sys.argv[1]
latest = max(glob.glob(os.path.join(metadata_dir, "v*.metadata.json")),
             key=lambda path: int(re.search(r"/v(\d+)\.metadata\.json$", path).group(1)))
with open(latest) as f:
    metadata = json.load(f)
snapshot = next(s for s in metadata["snapshots"] if s["snapshot-id"] == metadata["current-snapshot-id"])
print("next-row-id", metadata["next-row-id"], "first-row-id", snapshot["first-row-id"], "added-rows", snapshot["added-rows"])
PY
}

${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    INSERT INTO TABLE FUNCTION icebergLocal('${TABLE_PATH}', 'Parquet', 'x Int32') SELECT number + 20 FROM numbers(2);
"
print_state

# The second v3 commit keeps the assigned ids and continues after them.
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    INSERT INTO TABLE FUNCTION icebergLocal('${TABLE_PATH}', 'Parquet', 'x Int32') SELECT number + 30 FROM numbers(2);
"
print_state

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_v3_upgrade"
rm -rf "${TABLE_PATH}"
