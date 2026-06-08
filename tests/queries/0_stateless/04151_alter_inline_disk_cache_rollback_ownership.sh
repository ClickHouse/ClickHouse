#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check

# Regression for https://github.com/ClickHouse/ClickHouse/issues/63019
#
# clickhouse-gh[bot] follow-up review on PR #103818: the rollback path of an inline
# `disk(type = cache, name = ...)` validation must NOT remove a `FileCacheFactory` entry
# the rollback scope did not actually own. The companion 04145 test exercises the
# rollback when the cache name is BOTH a registered disk and a registered cache (the
# `getOrCreateDisk` existing-disk branch); the bot pointed out that this never reaches
# the `we_inserted_cache_entry == false` ownership decision in
# `Context::removePendingCustomDiskIfOwned`, because the existing-disk branch does not
# create a tentative registration.
#
# This test sets up the case the bot asked for explicitly: a pre-existing filesystem
# cache name that is NOT a registered disk, then a rejected ALTER whose inline
# `disk(type = cache, name = '<that pre-existing cache>', ...)` reaches the
# `getOrCreateDisk` new-disk branch, registers the disk tentatively, observes the
# pre-existing cache entry by path (`we_inserted_cache_entry` stays `false`), and is
# then rejected by the storage-policy migration guard. Asserts that after rollback the
# pre-existing cache is still visible by name in `system.filesystem_cache_settings`.
#
# The pre-existing cache `cache_for_rollback_test` is set up by
# `tests/config/config.d/storage_conf_04145.xml` with an absolute path under
# `<custom_cached_disks_base_directory>` (configured to
# `/var/lib/clickhouse/filesystem_caches/`), so an inline `disk(type = cache, name =
# 'cache_for_rollback_test', path = '/var/lib/clickhouse/filesystem_caches/cache_for_rollback_test/', ...)`
# resolves to the SAME path. `FileCacheFactory::getOrCreate` then matches by path and
# reuses the pre-existing entry instead of inserting a new one — the only way to reach
# `we_inserted_cache_entry == false` from user-facing SQL.
#
# A `.sh` test (rather than `.sql`) is needed because the absolute path in the inline
# `disk(...)` must match the cache's resolved path, which is environment-dependent
# (queried from `system.filesystem_cache_settings`).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# Sanity check: the pre-existing cache must have been loaded from the test config.
preexisting_count=$(${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM system.filesystem_cache_settings
    WHERE cache_name = 'cache_for_rollback_test'")
if [[ "$preexisting_count" != "1" ]]; then
    echo "FAIL: cache_for_rollback_test not preloaded (expected 1, got $preexisting_count)"
    exit 1
fi

# The cache's resolved absolute path. We use this as the inline disk's `path` so
# `FileCacheFactory::getOrCreate` matches by path and reuses the pre-existing entry.
preexisting_path=$(${CLICKHOUSE_CLIENT} --query "
    SELECT path
    FROM system.filesystem_cache_settings
    WHERE cache_name = 'cache_for_rollback_test'")

# Sibling table on its own object_storage disk. The rejected ALTER below tries to wrap
# this disk in a cache layer named `cache_for_rollback_test`. Storage-policy migration
# rejects the ALTER (cannot change a table's storage policy mid-life), and the rollback
# scope must not delete the pre-existing cache entry — its `we_inserted_cache_entry`
# flag is `false` because the cache existed before this scope opened.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04151_cache_rollback_sibling"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_04151_cache_rollback_sibling (a Int32) ENGINE = MergeTree() ORDER BY a
    SETTINGS disk = disk(
        name = '04151_cache_rollback_sibling_disk',
        type = object_storage,
        object_storage_type = local_blob_storage,
        path = './04151_cache_rollback_sibling_objstore/');"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04151_cache_rollback_sibling SELECT number FROM numbers(3)"

echo "before:"
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM system.filesystem_cache_settings
    WHERE cache_name = 'cache_for_rollback_test'"

# Rejected ALTER. The inline cache disk reuses the pre-existing `cache_for_rollback_test`
# cache entry by path; the disk is registered tentatively under that same name; then the
# storage-policy migration guard rejects with `BAD_ARGUMENTS` (changing a table's `disk`
# to a different storage policy is not supported in general). Without the
# `we_inserted_cache_entry` ownership check on rollback, the rollback path would call
# `FileCacheFactory::removeByName('cache_for_rollback_test')` and silently tear down the
# pre-existing cache.
${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE t_04151_cache_rollback_sibling MODIFY SETTING disk = disk(
        name = 'cache_for_rollback_test',
        type = cache,
        disk = '04151_cache_rollback_sibling_disk',
        path = '${preexisting_path}',
        max_size = '1Mi')" 2>&1 | grep -q "BAD_ARGUMENTS" && echo "alter rejected: BAD_ARGUMENTS"

echo "after:"
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM system.filesystem_cache_settings
    WHERE cache_name = 'cache_for_rollback_test'"

# The disk by name `cache_for_rollback_test` MUST have been rolled back from `DiskSelector`.
echo "disk rolled back:"
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM system.disks
    WHERE name = 'cache_for_rollback_test'"

# The sibling table must still be queryable on its original disk.
echo "sibling table works:"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a) FROM t_04151_cache_rollback_sibling"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_04151_cache_rollback_sibling"
