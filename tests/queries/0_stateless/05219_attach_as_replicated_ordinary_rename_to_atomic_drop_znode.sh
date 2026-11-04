#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database, no-shared-merge-tree

# A table of an `Ordinary` database converted to `ReplicatedMergeTree` keeps the fully expanded
# `/clickhouse/tables/<uuid>/<shard>` path as a literal. `RENAME TABLE` into an `Atomic` database gives
# the table a fresh UUID of its own, while the literal path keeps the minted one. `DROP TABLE` must still
# remove the emptied `/clickhouse/tables/<uuid>` parent znode after the table was reloaded with its new UUID.

# Creation of a database with Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ORDINARY_DB="ordinary_$CLICKHOUSE_DATABASE"

${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 -n -q "
    CREATE DATABASE $ORDINARY_DB ENGINE = Ordinary;
    CREATE TABLE $ORDINARY_DB.t_moved (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO $ORDINARY_DB.t_moved VALUES (1);

    DETACH TABLE $ORDINARY_DB.t_moved;
    ATTACH TABLE $ORDINARY_DB.t_moved AS REPLICATED;
    -- The conversion only rewrites the metadata; the znodes appear once the replica is restored.
    SYSTEM RESTORE REPLICA $ORDINARY_DB.t_moved;
"

ZOOKEEPER_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT zookeeper_path FROM system.replicas WHERE database = '$ORDINARY_DB' AND table = 't_moved'")
# /clickhouse/tables/<uuid>/<shard>: the parent of the replica znode is the UUID-named one the table owns.
PARENT_PATH=$(dirname "$ZOOKEEPER_PATH")
PARENT_NAME=$(basename "$PARENT_PATH")
GRANDPARENT_PATH=$(dirname "$PARENT_PATH")

${CLICKHOUSE_CLIENT} -q "SELECT 'path_shape', '$PARENT_NAME' LIKE '________-____-____-____-____________', '$GRANDPARENT_PATH'"

# The test database is `Atomic`: the move mints a UUID for the table, the explicit path stays as is.
${CLICKHOUSE_CLIENT} -n -q "
    RENAME TABLE $ORDINARY_DB.t_moved TO t_moved;
    SELECT 'renamed', uuid != toUUIDOrZero(''), zookeeper_path = '$ZOOKEEPER_PATH' FROM system.replicas WHERE database = currentDatabase() AND table = 't_moved';
    SELECT 'uuid_not_in_path', uuid != '$PARENT_NAME' FROM system.tables WHERE database = currentDatabase() AND name = 't_moved';

    -- A round trip through a plain re-attach resolves the path again with the new UUID, which is what a restart does.
    DETACH TABLE t_moved;
    ATTACH TABLE t_moved;
    SELECT 'reattached', engine, (SELECT count() FROM t_moved) FROM system.tables WHERE database = currentDatabase() AND name = 't_moved';
"

${CLICKHOUSE_CLIENT} -q "SELECT 'parent_before_drop', count() FROM system.zookeeper WHERE path = '$GRANDPARENT_PATH' AND name = '$PARENT_NAME'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_moved SYNC"

${CLICKHOUSE_CLIENT} -q "SELECT 'parent_after_drop', count() FROM system.zookeeper WHERE path = '$GRANDPARENT_PATH' AND name = '$PARENT_NAME'"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE $ORDINARY_DB"
