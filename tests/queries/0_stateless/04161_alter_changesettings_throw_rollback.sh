#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, zookeeper, long
#
# Regression for the clickhouse-gh[bot] review on PR #103818 (issue #63019), exception-safety
# gap when `MergeTreeData::changeSettings` itself throws AFTER mutating the live state.
#
# The settings-only ALTER branches call `changeSettings` against a caller-owned
# `CustomDiskRegistrationScope`. `changeSettings` mutates the live `storage_settings` and the
# in-memory metadata (`setInMemoryMetadata`) and THEN runs post-transition work
# (`startBackgroundMovesIfNeeded` / `startStatisticsCache`), both of which create and schedule
# pool tasks and can throw. Previously `changeSettings` was called OUTSIDE the caller's `try`,
# so a throw from that post-transition work skipped the catch-revert while the scope destructor
# still rolled the tentative inline disk back, leaving the live settings mutated and (on the
# inline-disk path) resolving to a disk no longer present in `DiskSelector`. A later successful
# ALTER would then persist those stale live settings.
#
# The fix moves `changeSettings` inside the same `try` as the metadata write (non-replicated and
# replicated settings-only branches) and arms the `SCOPE_EXIT` revert before `changeSettings` in
# the replicated ZooKeeper loop, so a throw anywhere inside `changeSettings` reverts the live
# settings first.
#
# This test injects the throw INSIDE `changeSettings` (after `setInMemoryMetadata`) with the ONCE
# failpoint `merge_tree_change_settings_fail_after_set_metadata`, which fires only in caller-owned
# mode. It first runs a settings ALTER that fails in that window, then a separate successful ALTER
# on a DIFFERENT setting. The persisted create query must show the failed ALTER's value reverted
# (not leaked). Run for both MergeTree and ReplicatedMergeTree.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Defense-in-depth cleanup. Each `run_case` enables the failpoint and disables it inline,
# but if the ALTER between enable and disable hangs or throws unexpectedly the server-wide
# failpoint could stay enabled and disrupt later tests on the same server. Always disable
# it and drop both per-label tables on exit. `DISABLE` on an inactive failpoint and DROP
# IF EXISTS are no-ops, so this is safe alongside the per-case cleanup.
trap '
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT merge_tree_change_settings_fail_after_set_metadata" 2>/dev/null || true
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_04161_mt SYNC; DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_04161_rmt SYNC;" 2>/dev/null || true
' EXIT

run_case() {
    local label=$1
    local engine=$2
    local tbl="${CLICKHOUSE_DATABASE}.t_04161_${label}"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${tbl} SYNC"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${tbl} (a Int32) ENGINE = ${engine} ORDER BY a
        SETTINGS merge_with_ttl_timeout = 100, min_bytes_for_wide_part = 200"

    # Inject a throw inside changeSettings, after it has mutated the live storage_settings and
    # in-memory metadata (the window of the post-transition work startBackgroundMovesIfNeeded /
    # startStatisticsCache). The ALTER applies merge_with_ttl_timeout = 777 to the live settings,
    # then the failpoint throws. The fix reverts the live settings on this path.
    ${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT merge_tree_change_settings_fail_after_set_metadata"
    echo -n "${label} alter_failed_injected: "
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${tbl} MODIFY SETTING merge_with_ttl_timeout = 777" 2>&1 \
        | grep -qE "FAULT_INJECTED" && echo yes || echo no
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT merge_tree_change_settings_fail_after_set_metadata"

    # A second, successful settings ALTER on a DIFFERENT setting. Its new in-memory metadata base
    # is the live state; if merge_with_ttl_timeout = 777 had leaked, it would be persisted here.
    # With the fix the failed ALTER was reverted, so only min_bytes_for_wide_part changes.
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${tbl} MODIFY SETTING min_bytes_for_wide_part = 999"

    echo -n "${label} ttl_reverted_to_100: "
    ${CLICKHOUSE_CLIENT} --query "
        SELECT countSubstrings(create_table_query, 'merge_with_ttl_timeout = 100') = 1
           AND countSubstrings(create_table_query, 'merge_with_ttl_timeout = 777') = 0
           AND countSubstrings(create_table_query, 'min_bytes_for_wide_part = 999') = 1
        FROM system.tables WHERE database = currentDatabase() AND name = 't_04161_${label}'"

    echo -n "${label} table_queryable: "
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${tbl}"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${tbl} SYNC"
}

run_case "mt" "MergeTree()"
run_case "rmt" "ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_04161_rmt', 'r1')"
