#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-shared-catalog
#
# `no-replicated-database` / `no-shared-merge-tree` / `no-shared-catalog`: the fixture edits the
# raw mutation znode of a `ReplicatedMergeTree` table at a fixed ZooKeeper path, and STOP MERGES
# must hold on the only replica that could execute the mutation.
#
# Regression test: a replicated mutation znode written by an older server version still scopes
# its commands with the original `IN PARTITION <value>` literal (new entries are rewritten to
# the `IN PARTITION ID` form at creation). When such an entry is loaded, the resolved partition
# scope is pinned from the block numbers allocated at its creation, so the mutation stays
# executable after a key-safe partition key type change (e.g. `Enum8 -> Int8`) even though the
# literal 'a' can no longer be decoded through the new key type. The legacy shape is fabricated
# by rewriting the znode of a pending mutation back to the literal form.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ZK_PATH="/clickhouse/tables/$CLICKHOUSE_DATABASE/t_legacy_mutation_znode"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_legacy_mutation_znode SYNC"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_legacy_mutation_znode (p Enum8('a' = 1, 'b' = 2), n Int64)
    ENGINE = ReplicatedMergeTree('$ZK_PATH', '1')
    PARTITION BY p ORDER BY tuple()"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_legacy_mutation_znode VALUES ('a', 1), ('b', 2)"

# Keep the mutation pending: it must still be pending when the partition key type changes.
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_legacy_mutation_znode"

${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE t_legacy_mutation_znode UPDATE n = n + 100 IN PARTITION 'a' WHERE 1"

# Fabricate the legacy format: rewrite the pinned `IN PARTITION ID '1'` scope of the znode back
# to the original literal, as an older server version would have written it. The commands text
# inside the znode keeps the quotes of the partition literal backslash-escaped, so the patterns
# are assembled with `char` (92 = backslash, 39 = quote) instead of fighting shell escaping.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO system.zookeeper (name, path, value)
    SELECT name, path, replace(value,
        concat('IN PARTITION ID ', char(92, 39), '1', char(92, 39)),
        concat('IN PARTITION ', char(92, 39), 'a', char(92, 39)))
    FROM system.zookeeper
    WHERE path = '$ZK_PATH/mutations'"

${CLICKHOUSE_CLIENT} --query "
    SELECT 'legacy znodes:', count() FROM system.zookeeper
    WHERE path = '$ZK_PATH/mutations'
      AND value LIKE concat('%IN PARTITION ', char(92, 39), 'a', char(92, 39), '%')"

# A key-safe metadata change of the partition key column: `Enum8 -> Int8` keeps the numeric
# on-disk partition id, but re-parsing the literal 'a' as `Int8` would throw.
${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE t_legacy_mutation_znode MODIFY COLUMN p Int8 SETTINGS alter_sync = 2"

# Simulate a restart so that the legacy entry is read back from ZooKeeper (this resets the
# merges blocker, so the mutation may execute right away; its partition scope has been pinned
# from the block numbers of the entry at load, without decoding the literal through the new
# partition key type).
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_legacy_mutation_znode"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_legacy_mutation_znode"

${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES t_legacy_mutation_znode"

# The pending legacy mutation is executed and affects only the partition it was scoped to.
# Without the pinning, its execution keeps failing to decode 'a' and this barrier times out.
${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE t_legacy_mutation_znode UPDATE n = n WHERE 1 SETTINGS mutations_sync = 2"

${CLICKHOUSE_CLIENT} --query "SELECT p, n FROM t_legacy_mutation_znode ORDER BY p, n"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_legacy_mutation_znode' AND NOT is_done"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_legacy_mutation_znode SYNC"
