#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, zookeeper, long
#
# Regression for clickhouse-gh[bot] review on PR #103818 (issue #63019), settings-only
# ALTER rollback gaps on StorageMergeTree and StorageReplicatedMergeTree.
#
# A settings-only `ALTER TABLE ... MODIFY SETTING ...` calls `changeSettings` against a
# caller-owned `CustomDiskRegistrationScope` BEFORE the metadata write
# (`DatabaseCatalog::alterTable`). `changeSettings` mutates the live in-memory
# `storage_settings` immediately. If the metadata write then throws, the settings-only
# branch used to leave the live in-memory settings mutated (unlike the full-ALTER branch,
# which reverts in a `catch`). A later successful ALTER would then persist those stale
# in-memory settings, and on the inline-disk path the scope destructor could roll a disk
# out from under live settings still referencing it.
#
# The fix reverts the live settings (and statistics metadata) on the metadata-write
# failure path of every settings-only branch, mirroring the full-ALTER `catch`.
#
# This test injects a metadata-write failure with the ONCE failpoint
# `database_ordinary_alter_table_fail` (thrown at the top of `DatabaseOrdinary::alterTable`,
# the common metadata-write path for Atomic and Replicated databases). It first runs a
# settings ALTER that fails (mutating `merge_with_ttl_timeout` in-memory), then a separate
# successful ALTER on a DIFFERENT setting. The persisted create query must show the failed
# ALTER's value reverted (not leaked). Run for both MergeTree and ReplicatedMergeTree.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

run_case() {
    local label=$1
    local engine=$2
    local tbl="${CLICKHOUSE_DATABASE}.t_04157_${label}"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${tbl} SYNC"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${tbl} (a Int32) ENGINE = ${engine} ORDER BY a
        SETTINGS merge_with_ttl_timeout = 100, min_bytes_for_wide_part = 200"

    # Inject a metadata-write failure during a settings-only ALTER. The ALTER applies
    # merge_with_ttl_timeout = 777 to the live in-memory settings, then the metadata write
    # throws. The fix reverts the in-memory settings on this path.
    ${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT database_ordinary_alter_table_fail"
    echo -n "${label} alter_failed_injected: "
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${tbl} MODIFY SETTING merge_with_ttl_timeout = 777" 2>&1 \
        | grep -qE "FAULT_INJECTED" && echo yes || echo no
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT database_ordinary_alter_table_fail"

    # A second, successful settings ALTER on a DIFFERENT setting. Its new in-memory metadata
    # base is the live state; if merge_with_ttl_timeout = 777 had leaked, it would be
    # persisted here. With the fix the failed ALTER was reverted, so only min_bytes changes.
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${tbl} MODIFY SETTING min_bytes_for_wide_part = 999"

    echo -n "${label} ttl_reverted_to_100: "
    ${CLICKHOUSE_CLIENT} --query "
        SELECT countSubstrings(create_table_query, 'merge_with_ttl_timeout = 100') = 1
           AND countSubstrings(create_table_query, 'merge_with_ttl_timeout = 777') = 0
           AND countSubstrings(create_table_query, 'min_bytes_for_wide_part = 999') = 1
        FROM system.tables WHERE database = currentDatabase() AND name = 't_04157_${label}'"

    echo -n "${label} table_queryable: "
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${tbl}"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${tbl} SYNC"
}

run_case "mt" "MergeTree()"
run_case "rmt" "ReplicatedMergeTree('/clickhouse/tables/${CLICKHOUSE_DATABASE}/t_04157_rmt', 'r1')"
