#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-replicated-database, no-shared-merge-tree
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
    type = object_storage,
    object_storage_type = local,
    metadata_type = plain_rewritable,
    path = 'disks/04326/${CLICKHOUSE_DATABASE}/')
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO 04326_t SELECT number, toString(number) FROM numbers(100)"

echo "-- the plain_rewritable disk is reported in system.remote_data_paths with metadata_type"
${CLICKHOUSE_CLIENT} --query "
SELECT count() >= 1 FROM system.remote_data_paths WHERE disk_name = '${disk_name}' AND metadata_type = 'PlainRewritable'"

echo "-- common_prefix_for_blobs is non-empty and remote_path starts with it"
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM system.remote_data_paths
WHERE disk_name = '${disk_name}' AND (common_prefix_for_blobs = '' OR NOT startsWith(remote_path, common_prefix_for_blobs))"

echo "-- non-root local data paths are present"
${CLICKHOUSE_CLIENT} --query "
SELECT count() >= 1 FROM system.remote_data_paths
WHERE disk_name = '${disk_name}' AND local_path != ''"

echo "-- a freshly written table has no ephemeral entries"
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM system.remote_data_paths
WHERE disk_name = '${disk_name}' AND is_ephemeral"

# A cache disk wraps the plain_rewritable metadata storage, but the common blob prefix must still be
# reported (it comes from the object storage contract, not a cast to the concrete metadata storage type).
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE 04326_cached_t (a Int32, b String) ORDER BY a
SETTINGS disk = disk(
    name = ${cached_disk_name},
    type = cache,
    max_size = '16Mi',
    path = '04326_cache_${CLICKHOUSE_DATABASE}/',
    disk = disk(
        name = 04326_cached_inner_${CLICKHOUSE_DATABASE},
        type = object_storage,
        object_storage_type = local,
        metadata_type = plain_rewritable,
        path = 'disks/04326cached/${CLICKHOUSE_DATABASE}/'))
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO 04326_cached_t SELECT number, toString(number) FROM numbers(100)"

echo "-- a wrapped (cache) plain_rewritable disk reports metadata_type and non-empty common_prefix_for_blobs"
${CLICKHOUSE_CLIENT} --query "
SELECT count() >= 1 FROM system.remote_data_paths
WHERE disk_name = '${cached_disk_name}' AND metadata_type = 'PlainRewritable' AND common_prefix_for_blobs != ''"

${CLICKHOUSE_CLIENT} --query "DROP TABLE 04326_t SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE 04326_cached_t SYNC"
