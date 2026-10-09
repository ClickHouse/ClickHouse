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
