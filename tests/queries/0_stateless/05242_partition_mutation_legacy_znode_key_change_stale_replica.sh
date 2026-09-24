#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-shared-catalog
#
# `no-replicated-database` / `no-shared-merge-tree` / `no-shared-catalog`: the fixture edits the
# raw mutation znode of a `ReplicatedMergeTree` table at a known ZooKeeper path.
#
# A partition key type change is refused while a legacy multi-partition mutation znode (with
# `IN PARTITION <value>` literals) is pending (see `05220`). The check must not rely on the local
# set of mutations of the replica running the `ALTER`, which is refreshed asynchronously: here the
# second replica has not loaded the legacy entry at all (its log pulling is stopped), and the change
# must still not be accepted there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_05242"
ZK_PATH="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$TABLE"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}_1 SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}_2 SYNC"

for replica in 1 2
do
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${TABLE}_$replica (p Enum8('a' = 1, 'b' = 2, 'c' = 3), n Int64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$TABLE', '$replica')
        PARTITION BY p ORDER BY tuple()"
done

${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE}_1 VALUES ('a', 1), ('b', 2), ('c', 3)"
${CLICKHOUSE_CLIENT} --query "SYSTEM SYNC REPLICA ${TABLE}_2"

# From now on the second replica does not load new mutation entries.
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP PULLING REPLICATION LOG ${TABLE}_2"

# The mutation fails on every part it applies to, so it stays pending.
${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE ${TABLE}_1 UPDATE n = throwIf(n > 0, 'keep the mutation pending') IN PARTITION 'a', 'b' WHERE 1"

# Fabricate the legacy format: rewrite the pinned `IN PARTITION ID '1', ID '2'` scope of the znode
# back to the original literals, as an older server version would have written it (see `05220`).
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

${CLICKHOUSE_CLIENT} --query "
    SELECT 'mutations known to the second replica:', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = '${TABLE}_2'"

# The second replica cannot refresh its view of the mutations while log pulling is stopped, so it
# refuses the change instead of accepting it on the stale view.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE}_2 MODIFY COLUMN p Int8 SETTINGS alter_sync = 0" 2>&1 \
    | grep -o "ABORTED" | head -n 1

${CLICKHOUSE_CLIENT} --query "SYSTEM START PULLING REPLICATION LOG ${TABLE}_2"

# Once it can load the legacy entry, the change is refused because of it.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE}_2 MODIFY COLUMN p Int8 SETTINGS alter_sync = 0" 2>&1 \
    | grep -o "ALTER_OF_COLUMN_IS_FORBIDDEN" | head -n 1

${CLICKHOUSE_CLIENT} --query "SELECT 'type:', toTypeName(p) FROM ${TABLE}_2 LIMIT 1"

${CLICKHOUSE_CLIENT} --query "KILL MUTATION WHERE database = currentDatabase() AND table = '${TABLE}_1' SYNC FORMAT Null"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}_1 SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}_2 SYNC"
