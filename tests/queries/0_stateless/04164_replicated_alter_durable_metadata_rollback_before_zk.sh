#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, zookeeper, long
#
# Regression test for the clickhouse-gh[bot] review on PR #103818 (issue #63019), durable-metadata
# rollback gap in the full ALTER path of StorageReplicatedMergeTree::alter (2026-06-17T09:11Z).
#
# A structural ALTER (e.g. ADD COLUMN) combined with MODIFY SETTING does not match the local-only
# settings/comment branches, so on a ReplicatedMergeTree it runs the full ZooKeeper-replicated ALTER
# loop. That loop writes the modified create query to the LOCAL metadata file via
# DatabaseCatalog::alterTable BEFORE committing to ZooKeeper with tryMulti. If tryMulti returns a
# non-ZOK error, or a later pre-commit step throws, the local metadata file is durably ahead of
# ZooKeeper.
#
# Before the fix the loop's SCOPE_EXIT restored only the in-memory settings/metadata, not the durable
# file: the live table read correctly, but the on-disk create query kept the failed setting. After a
# restart (or DETACH/ATTACH), the stale file was reloaded, bringing back the failed setting, and on
# the inline-disk path it would reference a disk the caller-owned scope had already rolled back. The
# fix rewrites the local metadata file back to the pre-ALTER state on the failure path (before the
# disk scope rolls any inline disk back), and disarms that rollback after a successful commit.
#
# This drives the failure deterministically with the ONCE failpoint
# replicated_alter_fail_after_local_metadata_before_zk, which throws inside the loop right after the
# local alterTable write and before tryMulti.
#
# Sequence: run a mixed ADD COLUMN + MODIFY SETTING ALTER that fails after the local write, then
# DETACH and ATTACH the table to force a reload from the durable metadata file. The reloaded create
# query must show the failed ALTER's setting reverted (back to its original value), proving the
# durable local file was rolled back, not left ahead of ZooKeeper. (The in-memory state alone would
# pass even without the fix, so the reload is the part that exposes the durable gap.)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TBL="${CLICKHOUSE_DATABASE}.t_04164_rmt"

# Defense-in-depth cleanup. The test enables the failpoint and disables it inline, but if the ALTER
# between enable and disable hangs or throws unexpectedly the server-wide failpoint could stay
# enabled and disrupt later tests on the same server. Always disable it and drop the table on exit.
# DISABLE on an inactive failpoint and DROP IF EXISTS are no-ops, so this is safe alongside the
# normal cleanup tail.
trap '
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT replicated_alter_fail_after_local_metadata_before_zk" 2>/dev/null || true
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${TBL}" 2>/dev/null || true
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TBL} SYNC" 2>/dev/null || true
' EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TBL} SYNC"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TBL} (a Int32) ENGINE =
        ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_04164_rmt', 'r1')
    ORDER BY a
    SETTINGS merge_with_ttl_timeout = 100, min_bytes_for_wide_part = 200"

# Inject a failure after the local metadata write and before the ZooKeeper commit, during a mixed
# ADD COLUMN + MODIFY SETTING ALTER (which routes through the full replicated loop). The local
# metadata file is written with merge_with_ttl_timeout = 777, then the failpoint throws. The fix
# rewrites the local file back to the pre-ALTER state.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT replicated_alter_fail_after_local_metadata_before_zk"
echo -n "alter_failed_injected: "
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TBL} ADD COLUMN b Int32, MODIFY SETTING merge_with_ttl_timeout = 777" 2>&1 \
    | grep -qE "FAULT_INJECTED" && echo yes || echo no
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT replicated_alter_fail_after_local_metadata_before_zk"

# Force a reload from the durable metadata file. Without the fix the file is still ahead of ZooKeeper
# (merge_with_ttl_timeout = 777), so the reloaded table shows the leaked setting. With the fix the
# file was rolled back, so it shows 100.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${TBL}"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${TBL}"

echo -n "ttl_reverted_to_100_after_reload: "
${CLICKHOUSE_CLIENT} --query "
    SELECT countSubstrings(create_table_query, 'merge_with_ttl_timeout = 100') = 1
       AND countSubstrings(create_table_query, 'merge_with_ttl_timeout = 777') = 0
    FROM system.tables WHERE database = currentDatabase() AND name = 't_04164_rmt'"

echo -n "table_queryable: "
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TBL}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TBL} SYNC"
