#!/usr/bin/env bash
# Tags: no-fasttest

# A merge-on-read table with position deletes (5 rows - 2 deleted): the trivial count is
# answered from the snapshot summary (`total-records` minus `total-position-deletes`).
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

# count() must not reuse the pre-delete cached per-file row count (5): the data file now
# has attached position deletes, so its cache entry must be ignored and a real scan must
# return 3. The settings are pinned because the test runner randomizes them; the
# count-from-files cache is deliberately left ENABLED so a regression in the
# attached-deletes guard resurfaces the stale cached value.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --optimize_trivial_count_query=1 --optimize_count_from_files=1 \
    --use_cache_for_count_from_files=1 --query "SELECT count() FROM ${TABLE}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --optimize_trivial_count_query=0 --optimize_count_from_files=1 \
    --use_cache_for_count_from_files=1 --query "SELECT count() FROM ${TABLE}"

# The trivial count optimization is applied from the snapshot summary.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --optimize_trivial_count_query=1 --query \
    "SELECT count() FROM (EXPLAIN SELECT count() FROM ${TABLE}) WHERE explain LIKE '%Optimized trivial count%'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
