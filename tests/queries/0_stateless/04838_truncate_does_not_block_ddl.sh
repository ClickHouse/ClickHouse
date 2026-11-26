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

truncate_error="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.truncate.stderr"
truncate_query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_truncate"
trap 'rm -f "$truncate_error"' EXIT

# With the queue stopped the DROP_RANGE entry is never executed, so TRUNCATE stays inside
# waitForLogEntryToBeProcessedIfNecessary. alter_sync is pinned because that wait is what the
# RENAME below must not block on; at alter_sync = 0 TRUNCATE would not wait at all.
$CLICKHOUSE_CLIENT --query_id="$truncate_query_id" -q "SET alter_sync = 1; TRUNCATE TABLE t" > /dev/null 2> "$truncate_error" &
truncate_pid=$!

# Stopping the queue stops executing it, not pulling into it, so the queue can still hold the
# GET_PART entry of the INSERT above. Only a DROP_RANGE is an entry TRUNCATE itself created.
queued=0
for _ in {1..300}
do
    queued=$($CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.replication_queue WHERE database = currentDatabase() AND table = 't' AND type = 'DROP_RANGE' SETTINGS use_query_cache = 0")
    [[ "$queued" == "1" ]] && break
    sleep 0.1
done
[[ "$queued" == "1" ]] || echo "TRUNCATE never created its DROP_RANGE entry"

# TRUNCATE only removes data, so DDL on the table name must not wait for it.
timeout 30 $CLICKHOUSE_CLIENT -q "RENAME TABLE t TO t2" && echo "RENAME is not blocked"

# dropPartitions creates every entry before it waits for any, so a queued entry does not mean
# TRUNCATE ever waited. After the RENAME, with the queues still stopped, a TRUNCATE that waited
# cannot have left the wait, and one that did not would have to outlive a whole RENAME to be here.
in_flight=$($CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.processes WHERE query_id = '$truncate_query_id' SETTINGS use_query_cache = 0")
[[ "$in_flight" == "1" ]] || echo "TRUNCATE did not stay in flight across the RENAME"

# Let TRUNCATE finish. Both names are started because the table kept its old name if the RENAME
# above was blocked, and starting the queues of a table that does not exist is a no-op.
$CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATION QUEUES t; SYSTEM START REPLICATION QUEUES t2"

# The count below says nothing about TRUNCATE unless TRUNCATE itself succeeded. A client killed
# without writing diagnostics still leaves its DROP_RANGE behind, so the count can match on its own.
truncate_status=0
wait "$truncate_pid" || truncate_status=$?
if [[ "$truncate_status" != "0" ]]
then
    echo "TRUNCATE client failed with status $truncate_status:" >&2
    cat "$truncate_error" >&2
    exit 1
fi
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t2"
