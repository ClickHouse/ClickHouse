#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, long
#
# Regression test for the clickhouse-gh[bot] review on PR #103818 (issue #63019),
# exception-safety gap in the FULL non-replicated ALTER path of StorageMergeTree::alter.
#
# A combined ALTER (a non-settings command such as ADD COLUMN together with
# MODIFY SETTING ...) routes through the full-ALTER branch, not the settings-only branch.
# That branch calls `changeSettings` (which mutates the live in-memory `storage_settings`)
# and then `checkTTLExpressions` / `setProperties` (which mutate the live in-memory
# metadata) BEFORE the metadata write `alterTable`. Previously `checkTTLExpressions` and
# `setProperties` ran OUTSIDE the `try`, so a throw between `changeSettings` and `alterTable`
# skipped the catch-revert: the live settings/metadata stayed mutated (and on the inline-disk
# path the `CustomDiskRegistrationScope` destructor would roll a freshly registered disk back
# while the live table still referenced it). A later successful ALTER would then persist those
# stale live settings.
#
# The fix moves `checkTTLExpressions` / `setProperties` inside the same `try` as `alterTable`,
# so any throw before the metadata write reverts the live settings and metadata first.
#
# This test drives a throw in exactly that window with the ONCE failpoint
# `storage_merge_tree_alter_fail_after_change_settings_before_metadata`, thrown after
# `changeSettings` / `setProperties` and before `alterTable`. It first runs a combined
# ALTER (ADD COLUMN + MODIFY SETTING merge_with_ttl_timeout = 777) that fails in that window,
# then a separate successful ALTER on a DIFFERENT setting. The persisted create query must
# show the failed ALTER's setting reverted (not leaked) and its column not added. Run for
# MergeTree. The non-replicated full-ALTER path is the one the bot flagged; the replicated
# ALTER loop already wraps its changeSettings call in a SCOPE_EXIT that reverts the live
# settings on any throw before the ZooKeeper commit, so the gap is specific to
# StorageMergeTree::alter.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

run_case() {
    local label=$1
    local engine=$2
    local tbl="${CLICKHOUSE_DATABASE}.t_04159_${label}"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${tbl} SYNC"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${tbl} (a Int32) ENGINE = ${engine} ORDER BY a
        SETTINGS merge_with_ttl_timeout = 100, min_bytes_for_wide_part = 200"

    # Combined ALTER (ADD COLUMN routes through the full-ALTER branch) that also changes a
    # setting. changeSettings mutates the live merge_with_ttl_timeout to 777 and setProperties
    # adds column b to the in-memory metadata, then the failpoint throws before alterTable.
    # The fix reverts both the live settings and the metadata on this path.
    ${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT storage_merge_tree_alter_fail_after_change_settings_before_metadata"
    echo -n "${label} alter_failed_injected: "
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${tbl} ADD COLUMN b Int32, MODIFY SETTING merge_with_ttl_timeout = 777" 2>&1 \
        | grep -qE "FAULT_INJECTED" && echo yes || echo no
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT storage_merge_tree_alter_fail_after_change_settings_before_metadata"

    # A second, successful settings ALTER on a DIFFERENT setting. Its new in-memory metadata
    # base is the live state; if merge_with_ttl_timeout = 777 or column b had leaked, they
    # would be persisted here. With the fix the failed ALTER was fully reverted.
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${tbl} MODIFY SETTING min_bytes_for_wide_part = 999"

    echo -n "${label} ttl_reverted_to_100: "
    ${CLICKHOUSE_CLIENT} --query "
        SELECT countSubstrings(create_table_query, 'merge_with_ttl_timeout = 100') = 1
           AND countSubstrings(create_table_query, 'merge_with_ttl_timeout = 777') = 0
           AND countSubstrings(create_table_query, 'min_bytes_for_wide_part = 999') = 1
        FROM system.tables WHERE database = currentDatabase() AND name = 't_04159_${label}'"

    echo -n "${label} column_b_not_added: "
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count() = 0 FROM system.columns
        WHERE database = currentDatabase() AND table = 't_04159_${label}' AND name = 'b'"

    echo -n "${label} table_queryable: "
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${tbl}"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${tbl} SYNC"
}

run_case "mt" "MergeTree()"
