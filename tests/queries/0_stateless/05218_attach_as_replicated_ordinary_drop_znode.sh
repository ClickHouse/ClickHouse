#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database, no-shared-merge-tree

# Converting a table of an `Ordinary` database to `ReplicatedMergeTree` expands `default_replica_path`
# (`/clickhouse/tables/{uuid}/{shard}` in the test config) with a freshly generated UUID and stores the result
# as a literal. `DROP TABLE` must still remove the emptied `/clickhouse/tables/<uuid>` parent znode, the way
# it does for a table of an `Atomic` database, whose path keeps the `{uuid}` macro.

# Creation of a database with Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ORDINARY_DB="ordinary_$CLICKHOUSE_DATABASE"

${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 -n -q "
    CREATE DATABASE $ORDINARY_DB ENGINE = Ordinary;
    CREATE TABLE $ORDINARY_DB.t_drop_znode (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO $ORDINARY_DB.t_drop_znode VALUES (1);

    DETACH TABLE $ORDINARY_DB.t_drop_znode;
    ATTACH TABLE $ORDINARY_DB.t_drop_znode AS REPLICATED;
    -- The conversion only rewrites the metadata; the znodes appear once the replica is restored.
    SYSTEM RESTORE REPLICA $ORDINARY_DB.t_drop_znode;
"

ZOOKEEPER_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT zookeeper_path FROM system.replicas WHERE database = '$ORDINARY_DB' AND table = 't_drop_znode'")
# /clickhouse/tables/<uuid>/<shard>: the parent of the replica znode is the UUID-named one the table owns.
PARENT_PATH=$(dirname "$ZOOKEEPER_PATH")
PARENT_NAME=$(basename "$PARENT_PATH")
GRANDPARENT_PATH=$(dirname "$PARENT_PATH")

${CLICKHOUSE_CLIENT} -q "SELECT 'path_shape', '$PARENT_NAME' LIKE '________-____-____-____-____________', '$GRANDPARENT_PATH'"
${CLICKHOUSE_CLIENT} -q "SELECT 'parent_before_drop', count() FROM system.zookeeper WHERE path = '$GRANDPARENT_PATH' AND name = '$PARENT_NAME'"

# A round trip through a plain re-attach reloads the literal path from metadata, which is what a restart does.
${CLICKHOUSE_CLIENT} -n -q "
    DETACH TABLE $ORDINARY_DB.t_drop_znode;
    ATTACH TABLE $ORDINARY_DB.t_drop_znode;
    SELECT 'reattached', engine, (SELECT count() FROM $ORDINARY_DB.t_drop_znode) FROM system.tables WHERE database = '$ORDINARY_DB' AND name = 't_drop_znode';

    DROP TABLE $ORDINARY_DB.t_drop_znode SYNC;
"

${CLICKHOUSE_CLIENT} -q "SELECT 'parent_after_drop', count() FROM system.zookeeper WHERE path = '$GRANDPARENT_PATH' AND name = '$PARENT_NAME'"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE $ORDINARY_DB"
