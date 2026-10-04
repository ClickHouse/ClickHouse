#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-shared-catalog
#
# `no-replicated-database` / `no-shared-merge-tree` / `no-shared-catalog`: the fixture edits the
# raw mutation znode of a `ReplicatedMergeTree` table at a known ZooKeeper path.
#
# A replicated mutation znode written by an older server version scopes its commands with the
# original `IN PARTITION <value>` literals. When such an entry spans several partitions, its scope
# cannot be recovered from the block numbers of the entry (unlike the single-partition case covered
# by `04847`), so a key-safe partition key type change (e.g. `Enum8 -> Int8`) would make the entry
# undecodable for every replica that loads it afterwards. The change is therefore refused while
# the entry is pending, and accepted again once the mutation is gone.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_legacy_multi_partition_znode"
ZK_PATH="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$TABLE"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $TABLE SYNC"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE $TABLE (p Enum8('a' = 1, 'b' = 2, 'c' = 3), n Int64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$TABLE', '1')
    PARTITION BY p ORDER BY tuple()"

${CLICKHOUSE_CLIENT} --query "INSERT INTO $TABLE VALUES ('a', 1), ('b', 2), ('c', 3)"

# The mutation fails on every part it applies to, so it stays pending without relying on
# `SYSTEM STOP MERGES` (which does not survive the DETACH / ATTACH below).
${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE $TABLE UPDATE n = throwIf(n > 0, 'keep the mutation pending') IN PARTITION 'a', 'b' WHERE 1"

# Fabricate the legacy format: rewrite the pinned `IN PARTITION ID '1', ID '2'` scope of the znode
# back to the original literals, as an older server version would have written it. The commands
# text inside the znode keeps the quotes of the partition literals backslash-escaped, so the
# patterns are assembled with `char` (92 = backslash, 39 = quote) instead of fighting shell escaping.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO system.zookeeper (name, path, value)
    SELECT name, path, replace(value,
        concat('IN PARTITION ID ', char(92, 39), '1', char(92, 39), ', ID ', char(92, 39), '2', char(92, 39)),
        concat('IN PARTITION ', char(92, 39), 'a', char(92, 39), ', ', char(92, 39), 'b', char(92, 39)))
    FROM system.zookeeper
    WHERE path = '$ZK_PATH/mutations'"

${CLICKHOUSE_CLIENT} --query "
    SELECT 'legacy znodes:', count() FROM system.zookeeper
    WHERE path = '$ZK_PATH/mutations'
      AND value LIKE concat('%IN PARTITION ', char(92, 39), 'a', char(92, 39), ', ', char(92, 39), 'b', char(92, 39), '%')"

# Simulate a restart so that the legacy entry is read back from ZooKeeper.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE $TABLE"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE $TABLE"

${CLICKHOUSE_CLIENT} --query "
    SELECT 'pending legacy mutations:', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = '$TABLE' AND NOT is_done"

# The partition key type change is refused while the legacy multi-partition entry is pending.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $TABLE MODIFY COLUMN p Int8 SETTINGS alter_sync = 2" 2>&1 \
    | grep -o "ALTER_OF_COLUMN_IS_FORBIDDEN" | head -n 1

# The literals still decode through the unchanged key, so the entry itself is fine to kill.
${CLICKHOUSE_CLIENT} --query "KILL MUTATION WHERE database = currentDatabase() AND table = '$TABLE' SYNC FORMAT Null"

# Without the legacy entry the same change is accepted.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $TABLE MODIFY COLUMN p Int8 SETTINGS alter_sync = 2"
${CLICKHOUSE_CLIENT} --query "SELECT toTypeName(p) FROM $TABLE LIMIT 1"
${CLICKHOUSE_CLIENT} --query "SELECT p, n FROM $TABLE ORDER BY p, n"

${CLICKHOUSE_CLIENT} --query "DROP TABLE $TABLE SYNC"
