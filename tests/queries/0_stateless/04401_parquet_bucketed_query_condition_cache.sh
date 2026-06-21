#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet and a working object storage (minio)

# Coverage for the query-condition-cache guard on the bucketed object-storage read
# path (`StorageObjectStorageSource`). When `cluster_table_function_split_granularity
# = 'bucket'`, a single Parquet object is fanned out into per-row-group buckets and
# each bucketed source reads only a subset of the object's row groups.
#
# The query condition cache is keyed by `(storage uuid, object path, condition hash)`
# and ignores the bucket id, so a bucketed source must NOT write its partial row-group
# match result into that whole-object key: `getMatchedBuckets` only reports the row
# groups that matched inside this bucket while the recorded `total_groups` is the whole
# file, so the row groups owned by other buckets would be stored as "unmatched" and a
# later read could skip valid row groups and return incomplete results. The write path
# now skips bucketed reads, mirroring the cache-read and count-cache guards.
#
# Note: the `*Cluster` table functions exercised here carry a nil storage uuid, for
# which `QueryConditionCache::{read,write}` are no-ops, so this test asserts that the
# bucketed and non-bucketed filtered reads agree on the full result; it guards the code
# path and documents the invariant for the day object-storage reads gain a real uuid.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ENDPOINT="http://localhost:11111/test/${CLICKHOUSE_DATABASE}"
AUTH="'test', 'testtest'"

# 400 rows at row-group size 50 => 8 row groups, so the bucket splitter (per-row-group
# buckets with the default batch size of 0) fans the object out into several bucketed sources.
WRITE_SETTINGS="s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 50"
# A predicate that matches a single value in the last row group (number = 375 lands in
# row group 7), i.e. outside the first bucket.
FILTER_SETTINGS="use_query_condition_cache = 1"
BUCKET_SETTINGS="${FILTER_SETTINGS}, cluster_table_function_split_granularity = 'bucket'"

obj="${ENDPOINT}/04401.parquet"

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3('${obj}', ${AUTH}, 'Parquet', 'number UInt64') SELECT number FROM numbers(400) SETTINGS ${WRITE_SETTINGS}"

# Bucketed filtered read: the matching row group is owned by a bucket other than the first.
${CLICKHOUSE_CLIENT} --query "SELECT number FROM s3Cluster('test_cluster_two_shards_localhost', '${obj}', ${AUTH}, 'Parquet') WHERE number = 375 ORDER BY number SETTINGS ${BUCKET_SETTINGS}"

# A subsequent non-bucketed read of the same predicate must still return the full result,
# i.e. the bucketed read above must not have poisoned the whole-object cache entry.
${CLICKHOUSE_CLIENT} --query "SELECT number FROM s3('${obj}', ${AUTH}, 'Parquet') WHERE number = 375 ORDER BY number SETTINGS ${FILTER_SETTINGS}"

# A broader predicate spanning several row groups, both bucketed and non-bucketed.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM s3Cluster('test_cluster_two_shards_localhost', '${obj}', ${AUTH}, 'Parquet') WHERE number % 137 = 0 SETTINGS ${BUCKET_SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM s3('${obj}', ${AUTH}, 'Parquet') WHERE number % 137 = 0 SETTINGS ${FILTER_SETTINGS}"
