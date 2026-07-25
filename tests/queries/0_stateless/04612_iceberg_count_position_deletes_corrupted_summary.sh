#!/usr/bin/env bash
# Tags: no-fasttest

# The snapshot summary's total-records hint is maintained incrementally by writers
# (parent total + added), so a corrupted commit in the table history poisons it for
# all subsequent snapshots. When the snapshot has live delete files, no metadata can
# produce an exact count (position delete records may be duplicated across delete
# files and the scan deduplicates them), so totalRows() must refuse the trivial count
# instead of trusting the (possibly corrupted) summary hint, and count() must fall
# back to a real scan.
#
# This test creates a merge-on-read table with position deletes (5 rows - 2 deleted),
# corrupts total-records from 5 to 100 and checks that count() returns 3 via a real
# scan (the summary-trusting code returned 100 - 2 = 98), and that the trivial count
# optimization is not applied.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id Int64, name String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "INSERT INTO ${TABLE} SELECT number, 'row' FROM numbers(5)"

# Merge-on-read delete: writes a position-delete file and a delete manifest.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "ALTER TABLE ${TABLE} DELETE WHERE id IN (1, 3)"

# Corrupt the total-records hint of the current snapshot in the latest metadata file,
# simulating a lossy commit earlier in the table history (observed in the wild).
# Note: no query is run before the corruption on purpose: metadata files are immutable
# per the Iceberg spec, so the server legitimately caches their parsed state by path.
python3 - "${TABLE_PATH}" << 'EOF'
import glob
import json
import re
import sys

files = glob.glob(sys.argv[1] + "metadata/*.metadata.json")
assert files, "no metadata files found"

def version(path):
    match = re.search(r"(\d+)[^/]*\.metadata\.json$", path)
    return int(match.group(1)) if match else -1

latest = max(files, key=version)
with open(latest) as f:
    metadata = json.load(f)
current_snapshot_id = metadata["current-snapshot-id"]
for snapshot in metadata["snapshots"]:
    if snapshot["snapshot-id"] == current_snapshot_id:
        assert snapshot["summary"]["total-records"] == "5", snapshot["summary"]
        snapshot["summary"]["total-records"] = "100"
with open(latest, "w") as f:
    json.dump(metadata, f)
EOF

# count() must not trust the corrupted summary. The setting is pinned because the
# test runner randomizes it.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --optimize_trivial_count_query=1 --query "SELECT count() FROM ${TABLE}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --optimize_trivial_count_query=0 --query "SELECT count() FROM ${TABLE}"

# With live position delete files no metadata count is exact, so the trivial count
# optimization must not be applied even when explicitly enabled.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --optimize_trivial_count_query=1 --query \
    "SELECT count() FROM (EXPLAIN SELECT count() FROM ${TABLE}) WHERE explain LIKE '%Optimized trivial count%'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
