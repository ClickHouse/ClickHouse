#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-object-storage, no-replicated-database, no-shared-merge-tree
# Tag no-fasttest: requires S3
# Tag no-shared-merge-tree: does not support replication
# Tag no-replicated-database: plain rewritable should not be shared between replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_name="04326_disk_${CLICKHOUSE_DATABASE}"
cached_disk_name="04326_cached_disk_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS 04326_t SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS 04326_cached_t SYNC"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE 04326_t (a Int32, b String) ORDER BY a
SETTINGS disk = disk(
    name = ${disk_name},
    type = s3_plain_rewritable,
    endpoint = 'http://localhost:11111/test/04326/${CLICKHOUSE_DATABASE}/',
    access_key_id = clickhouse,
    secret_access_key = clickhouse)
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO 04326_t SELECT number, toString(number) FROM numbers(100)"

echo "-- the plain_rewritable disk is reported in system.remote_data_paths with metadata_type"
${CLICKHOUSE_CLIENT} --query "
SELECT count() >= 1 FROM system.remote_data_paths WHERE disk_name = '${disk_name}' AND metadata_type = 'PlainRewritable'"

echo "-- non-root local data paths are present"
${CLICKHOUSE_CLIENT} --query "
SELECT count() >= 1 FROM system.remote_data_paths
WHERE disk_name = '${disk_name}' AND local_path != ''"

echo "-- last_modified is populated"
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM system.remote_data_paths WHERE disk_name = '${disk_name}' AND last_modified = 0"

# An object that only exists while an operation is in flight is named `_tmp_...`, so a leftover of an
# interrupted operation is visible as such in the path. A table nothing has interrupted has none.
echo "-- a freshly written table has no temporary objects left behind"
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM system.remote_data_paths
WHERE disk_name = '${disk_name}' AND local_path LIKE '%\_tmp\_%'"

# A cache disk wraps the plain_rewritable metadata storage; the wrapped disk must still be reported.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE 04326_cached_t (a Int32, b String) ORDER BY a
SETTINGS disk = disk(
    name = ${cached_disk_name},
    type = cache,
    max_size = '16Mi',
    path = '04326_cache_${CLICKHOUSE_DATABASE}/',
    disk = disk(
        name = 04326_cached_inner_${CLICKHOUSE_DATABASE},
        type = s3_plain_rewritable,
        endpoint = 'http://localhost:11111/test/04326cached/${CLICKHOUSE_DATABASE}/',
        access_key_id = clickhouse,
        secret_access_key = clickhouse))
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO 04326_cached_t SELECT number, toString(number) FROM numbers(100)"

echo "-- a wrapped (cache) plain_rewritable disk reports metadata_type"
${CLICKHOUSE_CLIENT} --query "
SELECT count() >= 1 FROM system.remote_data_paths
WHERE disk_name = '${cached_disk_name}' AND metadata_type = 'PlainRewritable'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE 04326_t SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE 04326_cached_t SYNC"
