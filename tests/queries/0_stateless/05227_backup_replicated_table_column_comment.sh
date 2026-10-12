#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BACKUP_ID="${CLICKHOUSE_DATABASE}_column_comment"

# A comment ALTER is not replicated through ZooKeeper, so the columns node of the table does not know the
# comment, while the backup takes the column list from that node.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t SYNC;
    CREATE TABLE t (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') ORDER BY id;
    ALTER TABLE t COMMENT COLUMN id 'ct';
    BACKUP TABLE t TO Disk('backups', '$BACKUP_ID') FORMAT Null;
    -- SYNC, so that the RESTORE below can create the table at the same ZooKeeper path.
    DROP TABLE t SYNC;
    RESTORE TABLE t FROM Disk('backups', '$BACKUP_ID') FORMAT Null;
    SELECT comment FROM system.columns WHERE database = currentDatabase() AND table = 't' AND name = 'id';
    DROP TABLE t SYNC;
"
