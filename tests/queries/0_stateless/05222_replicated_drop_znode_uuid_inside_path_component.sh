#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree

# The `default_replica_path` template may put the `{uuid}` macro inside a path component, as in
# `/clickhouse/tables/pika{uuid}chu/{shard}`. A table converted to `ReplicatedMergeTree` in an `Ordinary`
# database stores the fully expanded path as a literal, so after a re-attach the `{uuid}` macro is no
# longer there to mark the owned znode. `DROP TABLE` must still recover the UUID from inside the
# component and remove the emptied `pika<uuid>chu` znode, without touching the znodes above it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

UUID=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")
# The same literal path an `Ordinary` conversion with such a template would have stored.
ZK_PREFIX="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX"
ZOOKEEPER_PATH="$ZK_PREFIX/pika${UUID}chu/s1"

${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE t_uuid_inside (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/pika${UUID}chu/s1', 'r1') ORDER BY x;
    INSERT INTO t_uuid_inside VALUES (1);

    -- A round trip through a plain re-attach resolves the path again from the literal, which is what a restart does.
    DETACH TABLE t_uuid_inside;
    ATTACH TABLE t_uuid_inside;
    SELECT 'reattached', (SELECT count() FROM t_uuid_inside), zookeeper_path = '$ZOOKEEPER_PATH' FROM system.replicas WHERE database = currentDatabase() AND table = 't_uuid_inside';
"

${CLICKHOUSE_CLIENT} -q "SELECT 'parent_before_drop', count() FROM system.zookeeper WHERE path = '$ZK_PREFIX' AND name = 'pika${UUID}chu'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_uuid_inside SYNC"

${CLICKHOUSE_CLIENT} -q "SELECT 'parent_after_drop', count() FROM system.zookeeper WHERE path = '$ZK_PREFIX' AND name = 'pika${UUID}chu'"
# The owned prefix ends with the component containing the UUID: the znode above it is left alone.
${CLICKHOUSE_CLIENT} -q "SELECT 'grandparent_after_drop', count() FROM system.zookeeper WHERE path = '/clickhouse/tables' AND name = '$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX'"
