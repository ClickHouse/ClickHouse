#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t SYNC;
    DROP TABLE IF EXISTS t2 SYNC;
    CREATE TABLE t (a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') ORDER BY a;
    INSERT INTO t VALUES (1);
    SYSTEM STOP REPLICATION QUEUES t;
"

# With the queue stopped the DROP_RANGE entry is never executed, so TRUNCATE stays inside
# waitForLogEntryToBeProcessedIfNecessary.
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t" > /dev/null 2>&1 &

# The entry exists, so TRUNCATE created it and is now waiting for it to be processed.
for _ in {1..300}
do
    [[ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.replication_queue WHERE database = currentDatabase() AND table = 't'")" != "0" ]] && break
    sleep 0.1
done

# TRUNCATE only removes data, so DDL on the table name must not wait for it.
timeout 30 $CLICKHOUSE_CLIENT -q "RENAME TABLE t TO t2" && echo "RENAME is not blocked"

# Let TRUNCATE finish. Both names are started because the table kept its old name if the RENAME
# above was blocked, and starting the queues of a table that does not exist is a no-op.
$CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATION QUEUES t; SYSTEM START REPLICATION QUEUES t2"
wait
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t2"
