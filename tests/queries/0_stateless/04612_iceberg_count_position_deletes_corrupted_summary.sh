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
#
# It also pins the count-from-files cache contract: the cache is primed with the raw
# per-file row count (5) before the delete, and the post-delete counts run with the
# cache enabled, so reusing a cached row count for a file whose delete state changed
# (the cache key is only path + mtime, both untouched by the delete) fails the test.

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

# Prime the per-file count cache before any delete file exists: this count-only scan
# stores the raw row count of the data file (5) keyed by its path + modification time.
# The ALTER DELETE below adds a position-delete file WITHOUT touching the data file, so
# that cache key stays valid; the post-delete counts then prove the stale pre-delete
# value is not reused (a regression returns 5 instead of 3 there). The settings are
# pinned because the test runner randomizes them.
${CLICKHOUSE_CLIENT} --optimize_trivial_count_query=0 --optimize_count_from_files=1 --use_cache_for_count_from_files=1 \
    --query "SELECT count() FROM ${TABLE}"

# Merge-on-read delete: writes a position-delete file and a delete manifest.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "ALTER TABLE ${TABLE} DELETE WHERE id IN (1, 3)"

# Corrupt the total-records hint of the current snapshot in the latest metadata file,
# simulating a lossy commit earlier in the table history (observed in the wild).
# Note: nothing reads this metadata file before the corruption on purpose: metadata
# files are immutable per the Iceberg spec, so the server legitimately caches their
# parsed state by path. (The cache-priming count above only saw the pre-delete
# metadata file; the file corrupted here is created by the ALTER DELETE commit.)
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

# count() must not trust the corrupted summary (100 - 2 = 98) and must not reuse the
# pre-delete cached per-file row count (5): the data file now has attached position
# deletes, so its cache entry must be ignored and a real scan must return 3. The
# settings are pinned because the test runner randomizes them; the count-from-files
# cache is deliberately left ENABLED so a regression in the attached-deletes guard
# resurfaces the stale cached value.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --optimize_trivial_count_query=1 --optimize_count_from_files=1 \
    --use_cache_for_count_from_files=1 --query "SELECT count() FROM ${TABLE}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --optimize_trivial_count_query=0 --optimize_count_from_files=1 \
    --use_cache_for_count_from_files=1 --query "SELECT count() FROM ${TABLE}"

# With live position delete files no metadata count is exact, so the trivial count
# optimization must not be applied even when explicitly enabled.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --optimize_trivial_count_query=1 --query \
    "SELECT count() FROM (EXPLAIN SELECT count() FROM ${TABLE}) WHERE explain LIKE '%Optimized trivial count%'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
