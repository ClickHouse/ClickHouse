#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3

# Regression test for ETag propagation to s3Cluster workers on the bucket-splitting path.
#
# With `cluster_table_function_split_granularity='bucket'` and `s3_validate_etag_on_read=1`, the
# coordinator reads the object once (a HEAD to refresh the skip_object_metadata placeholder and
# capture the ETag) to compute bucket boundaries. The captured metadata is now propagated to the
# worker via `ClusterFunctionReadTaskResponse`, so the worker pins its read to the SAME generation
# instead of issuing its own HEAD and validating against a possibly newer generation.
#
# Observable: the worker (secondary query, is_initial_query = 0) no longer issues ANY S3 HEAD - it
# reuses the metadata the coordinator propagated. Before the fix the worker re-`HEAD`ed the object,
# so the secondary query's `S3HeadObject` was >= 1. Asserting the worker-side count is 0 is robust:
# it does not depend on how many HEADs the coordinator (initiator) does for listing/splitting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

parq="http://localhost:11111/test/04417_etag_${CLICKHOUSE_DATABASE}.parquet"
auth="'test', 'testtest'"

${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3('$parq', $auth, 'Parquet') SELECT number AS n FROM numbers(100) SETTINGS s3_truncate_on_insert=1"

qid="04417_split_headcount_${CLICKHOUSE_DATABASE}"

# Explicit structure ('n UInt64') bypasses schema inference (which would HEAD the object too), so the
# only HEADs come from the bucket-split read path. Filesystem cache is disabled to avoid extra
# metadata lookups.
${CLICKHOUSE_CLIENT} --query_id "$qid" -q "
    SELECT count() FROM s3Cluster('test_cluster_one_shard_three_replicas_localhost', '$parq', $auth, 'Parquet', 'n UInt64')
    SETTINGS s3_validate_etag_on_read = 1, cluster_table_function_split_granularity = 'bucket', enable_filesystem_cache = 0, use_cache_for_count_from_files = 0
" > /dev/null

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

# Sum HEADs done by the worker (secondary) queries only. Expected: 0 (worker reuses the propagated
# metadata). Before the fix the worker re-HEAD made this >= 1.
${CLICKHOUSE_CLIENT} -q "
    SELECT sum(ProfileEvents['S3HeadObject'])
    FROM system.query_log
    WHERE initial_query_id = '$qid' AND is_initial_query = 0 AND type = 'QueryFinish' AND event_date >= today() - 1
"

# When the `_tags` virtual column is requested, the propagated metadata (which carries no tags) is not
# enough: the worker must still fetch the object's tags. Assert it does at least one HEAD in that case,
# so propagation never silently returns empty `_tags` on the bucket-split path (it keeps the pinned
# ETag and only adds the freshly fetched tags).
qid_tags="04417_split_tags_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "$qid_tags" -q "
    SELECT count() FROM s3Cluster('test_cluster_one_shard_three_replicas_localhost', '$parq', $auth, 'Parquet', 'n UInt64')
    WHERE NOT ignore(_tags)
    SETTINGS s3_validate_etag_on_read = 1, cluster_table_function_split_granularity = 'bucket', enable_filesystem_cache = 0, use_cache_for_count_from_files = 0
" > /dev/null

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} -q "
    SELECT sum(ProfileEvents['S3HeadObject']) >= 1
    FROM system.query_log
    WHERE initial_query_id = '$qid_tags' AND is_initial_query = 0 AND type = 'QueryFinish' AND event_date >= today() - 1
"
