#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-replicated-database, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A replicated ALTER writes the columns node in ZooKeeper before the replica applies it, so a backup taken in
# that window must keep the comments from that node, which is what the replica gets once it applies the ALTER.
n=0
for alter in \
    "MODIFY COLUMN c COMMENT 'new', ADD COLUMN d UInt64" \
    "MODIFY COLUMN c REMOVE COMMENT" \
    "DROP COLUMN c, RENAME COLUMN y TO c"
do
    n=$((n + 1))
    backup="Disk('backups', '${CLICKHOUSE_DATABASE}_lagging_column_comment_$n')"
    echo "$alter"
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE t (k UInt64, c UInt64 COMMENT 'old', y UInt64 COMMENT 'y', z UInt64)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') ORDER BY k;
        ALTER TABLE t COMMENT COLUMN z 'local';
        SYSTEM STOP REPLICATION QUEUES t;
        ALTER TABLE t $alter SETTINGS alter_sync = 0;
        BACKUP TABLE t TO $backup SETTINGS structure_only = 1 FORMAT Null;
        SYSTEM START REPLICATION QUEUES t;
        SYSTEM SYNC REPLICA t;
        SELECT 'source', name, comment FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY position;
        DROP TABLE t SYNC;
        RESTORE TABLE t FROM $backup FORMAT Null;
        SELECT 'restored', name, comment FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY position;
        DROP TABLE t SYNC;
    "
done

wait_for()
{
    for _ in {1..600}; do
        [ "$($CLICKHOUSE_CLIENT --query "$1")" = "$2" ] && return
        sleep 0.1
    done
    echo "Timeout waiting for: $1"
}

# A restart between the ZooKeeper write and the local apply of an ALTER_METADATA sets the replica's metadata version
# before the entry is applied. Forge that state; the entry stays queued behind a mutation that cannot finish.
echo "restart before the queued ALTER_METADATA is applied"
zk="/clickhouse/tables/${CLICKHOUSE_DATABASE}/t"
queued="SELECT count() FROM system.replication_queue WHERE database = currentDatabase() AND table = 't' AND type = 'ALTER_METADATA'"
backup="Disk('backups', '${CLICKHOUSE_DATABASE}_lagging_column_comment_restart')"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t (k UInt64, c UInt64 COMMENT 'old', x String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') ORDER BY k;
    INSERT INTO t VALUES (1, 1, 'abc');
    ALTER TABLE t MODIFY COLUMN x UInt8 SETTINGS alter_sync = 0;
"
wait_for "SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't' AND name = 'x'" "UInt8"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t MODIFY COLUMN c REMOVE COMMENT SETTINGS alter_sync = 0"
wait_for "$queued" "1"
$CLICKHOUSE_CLIENT --query "
    DETACH TABLE t;
    INSERT INTO system.zookeeper (name, path, value)
        SELECT name, '$zk/replicas/r1', value FROM system.zookeeper WHERE path = '$zk' AND name IN ('columns', 'metadata')
        UNION ALL SELECT 'metadata_version', '$zk/replicas/r1', '2';
    ATTACH TABLE t;
"
wait_for "SELECT is_readonly FROM system.replicas WHERE database = currentDatabase() AND table = 't'" "0"
$CLICKHOUSE_CLIENT --query "
    SELECT 'before backup', comment, ($queued) FROM system.columns WHERE database = currentDatabase() AND table = 't' AND name = 'c';
    BACKUP TABLE t TO $backup SETTINGS structure_only = 1 FORMAT Null;
    KILL MUTATION WHERE database = currentDatabase() AND table = 't' FORMAT Null;
"
wait_for "$queued" "0"
$CLICKHOUSE_CLIENT --query "
    SELECT 'source', name, comment FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY position;
    DROP TABLE t SYNC;
    RESTORE TABLE t FROM $backup FORMAT Null;
    SELECT 'restored', name, comment FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY position;
    DROP TABLE t SYNC;
"
