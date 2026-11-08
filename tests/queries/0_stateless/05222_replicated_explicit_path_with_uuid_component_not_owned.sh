#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree

# A table of an `Ordinary` database stores its expanded ZooKeeper path literally, so the znode it owns is
# recovered by matching that path against the `default_replica_path` template again. A path the user wrote
# by hand does not come from that template, even when one of its components looks like a UUID (a per-tenant
# znode, say), so it keeps the meaning it always had: `DROP TABLE` removes the table's own znode and leaves
# everything above it alone.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

UUID=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")

${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE t_explicit_uuid (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/tenant-$UUID/main', 'r1') ORDER BY x;
    INSERT INTO t_explicit_uuid VALUES (1);

    -- A round trip through a plain re-attach resolves the path again from the literal, which is what a restart does.
    DETACH TABLE t_explicit_uuid;
    ATTACH TABLE t_explicit_uuid;
    SELECT 'reattached', (SELECT count() FROM t_explicit_uuid);
"

${CLICKHOUSE_CLIENT} -q "SELECT 'parent_before_drop', count() FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX' AND name = 'tenant-$UUID'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_explicit_uuid SYNC"

# The UUID-shaped component is state of whoever wrote the path, not a boundary this table owns.
${CLICKHOUSE_CLIENT} -q "SELECT 'parent_after_drop', count() FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX' AND name = 'tenant-$UUID'"
${CLICKHOUSE_CLIENT} -q "SELECT 'own_znode_after_drop', count() FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/tenant-$UUID' AND name = 'main'"
