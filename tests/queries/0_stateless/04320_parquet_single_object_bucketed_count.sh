#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet and a working object storage (minio)

# Regression test for the object-storage count-cache guards on the bucketed read
# path (`StorageObjectStorageSource`). When `cluster_table_function_split_granularity
# = 'bucket'`, a single Parquet object is fanned out into per-row-group buckets and
# each bucketed source reads only a subset of the object. The count cache is keyed by
# object path and ignores the bucket id, so:
#   * the read side must NOT consult a whole-object cached count for a bucketed source
#     (otherwise every bucket reports the full total and `count()` is multiplied by the
#     number of buckets), and
#   * the write side must NOT store a bucketed source's partial count under the
#     whole-object key (otherwise a later whole-object `count()` reads a partial value).
# This mirrors the `StorageFile` coverage in `04230_parquet_single_file_parallel_count`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ENDPOINT="http://localhost:11111/test/${CLICKHOUSE_DATABASE}"
AUTH="'test', 'testtest'"

# 400 rows at row-group size 50 => 8 row groups, so the bucket splitter (per-row-group
# buckets with the default batch size of 0) fans the object out into several bucketed sources.
WRITE_SETTINGS="s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 50"
READ_SETTINGS="optimize_count_from_files = 1, use_cache_for_count_from_files = 1, cluster_table_function_split_granularity = 'bucket'"

# --- Read side -------------------------------------------------------------------
read_obj="${ENDPOINT}/04320_read.parquet"

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3('${read_obj}', ${AUTH}, 'Parquet', 'number UInt64') SELECT number FROM numbers(400) SETTINGS ${WRITE_SETTINGS}"

# Populate the whole-object count cache with a non-bucketed read first.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM s3('${read_obj}', ${AUTH}, 'Parquet') SETTINGS optimize_count_from_files = 1, use_cache_for_count_from_files = 1"

# Bucketed cluster count must return the true total, not total * number_of_buckets.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM s3Cluster('test_cluster_two_shards_localhost', '${read_obj}', ${AUTH}, 'Parquet') SETTINGS ${READ_SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM s3Cluster('test_cluster_two_shards_localhost', '${read_obj}', ${AUTH}, 'Parquet') SETTINGS ${READ_SETTINGS}"

# --- Write side ------------------------------------------------------------------
write_obj="${ENDPOINT}/04320_write.parquet"

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3('${write_obj}', ${AUTH}, 'Parquet', 'number UInt64') SELECT number FROM numbers(400) SETTINGS ${WRITE_SETTINGS}"

# An unfiltered bucketed data read must not write any per-bucket partial count into the
# whole-object cache (optimize_count_from_files = 0 so this is a real data read).
${CLICKHOUSE_CLIENT} --query "SELECT sum(number) FROM s3Cluster('test_cluster_two_shards_localhost', '${write_obj}', ${AUTH}, 'Parquet') SETTINGS optimize_count_from_files = 0, use_cache_for_count_from_files = 1, cluster_table_function_split_granularity = 'bucket'"

# A subsequent whole-object count must still return the true total, not a poisoned
# per-bucket value left behind by the bucketed read above.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM s3('${write_obj}', ${AUTH}, 'Parquet') SETTINGS optimize_count_from_files = 1, use_cache_for_count_from_files = 1"
