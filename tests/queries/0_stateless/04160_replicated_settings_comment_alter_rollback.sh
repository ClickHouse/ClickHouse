#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, zookeeper, long
#
# Regression test for the clickhouse-gh[bot] review on PR #103818 (issue #63019),
# exception-safety gap in the mixed settings/comment ALTER path of
# StorageReplicatedMergeTree::alter (the areNonReplicatedAlterCommands branch).
#
# A batch that mixes a setting and a comment (e.g. MODIFY COMMENT ..., MODIFY SETTING ...)
# matches neither isSettingsAlter() nor isCommentAlter(), so on a ReplicatedMergeTree it
# routes through the local areNonReplicatedAlterCommands() branch instead of the ZooKeeper
# replicated path. That branch called changeSettings (which mutates the live in-memory
# storage_settings and, on the inline-disk path, registers a fresh disk and committed it
# immediately) and then wrote metadata via alterTable WITHOUT a try/catch. A throw in
# alterTable left the live settings mutated (and the inline disk committed) although the
# metadata write never became durable; a later successful ALTER then persisted the stale
# live settings.
#
# The fix gives this branch the same caller-owned CustomDiskRegistrationScope plus
# revert-before-unwind pattern as the isSettingsAlter() branch above it: changeSettings runs
# against a caller-owned scope, and a throw in alterTable restores the old settings/metadata
# before the scope rolls the inline disk back, with the scope committed only after alterTable
# succeeds. The sibling isSettingsAlter() and ZooKeeper paths were already protected; the gap
# was specific to this mixed-command branch. StorageMergeTree has no equivalent branch (a
# mixed ALTER there routes through the protected full-ALTER path, covered by 04159).
#
# This test drives a metadata-write failure with the ONCE failpoint
# database_ordinary_alter_table_fail (thrown at the top of DatabaseOrdinary::alterTable, the
# metadata-write path inherited by the Atomic/Replicated databases). It runs a mixed
# MODIFY COMMENT + MODIFY SETTING ALTER that fails, then a separate successful ALTER on a
# DIFFERENT setting. The persisted create query must show the failed ALTER's setting reverted
# (not leaked).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TBL="${CLICKHOUSE_DATABASE}.t_04160_rmt"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TBL} SYNC"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TBL} (a Int32) ENGINE =
        ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_04160_rmt', 'r1')
    ORDER BY a
    SETTINGS merge_with_ttl_timeout = 100, min_bytes_for_wide_part = 200"

# Inject a metadata-write failure during a mixed comment+settings ALTER. The ALTER applies
# merge_with_ttl_timeout = 777 to the live in-memory settings (and the comment to the live
# metadata) via the areNonReplicatedAlterCommands branch, then the metadata write throws.
# The fix reverts the in-memory settings on this path.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT database_ordinary_alter_table_fail"
echo -n "alter_failed_injected: "
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TBL} MODIFY COMMENT 'failed alter comment', MODIFY SETTING merge_with_ttl_timeout = 777" 2>&1 \
    | grep -qE "FAULT_INJECTED" && echo yes || echo no
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT database_ordinary_alter_table_fail"

# A second, successful settings ALTER on a DIFFERENT setting. Its new in-memory metadata base
# is the live state; if merge_with_ttl_timeout = 777 had leaked, it would be persisted here.
# With the fix the failed ALTER was reverted, so only min_bytes changes.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TBL} MODIFY SETTING min_bytes_for_wide_part = 999"

echo -n "ttl_reverted_to_100: "
${CLICKHOUSE_CLIENT} --query "
    SELECT countSubstrings(create_table_query, 'merge_with_ttl_timeout = 100') = 1
       AND countSubstrings(create_table_query, 'merge_with_ttl_timeout = 777') = 0
       AND countSubstrings(create_table_query, 'min_bytes_for_wide_part = 999') = 1
    FROM system.tables WHERE database = currentDatabase() AND name = 't_04160_rmt'"

echo -n "comment_not_leaked: "
${CLICKHOUSE_CLIENT} --query "
    SELECT comment = '' FROM system.tables WHERE database = currentDatabase() AND name = 't_04160_rmt'"

echo -n "table_queryable: "
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TBL}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TBL} SYNC"
