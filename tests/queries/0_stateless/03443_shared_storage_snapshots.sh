#!/usr/bin/env bash
# Tags: long, no-parallel, no-parallel-replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT=${CLICKHOUSE_CLIENT:-clickhouse-client}

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS events"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE events
(
    user_id UInt32,
    value UInt32
) ENGINE = MergeTree()
ORDER BY user_id"

$CLICKHOUSE_CLIENT --query "INSERT INTO events VALUES (1, 100)"
$CLICKHOUSE_CLIENT --query "INSERT INTO events VALUES (2, 200)"

# Run query with snapshot sharing enabled
RESULT=$(
    $CLICKHOUSE_CLIENT --query "
        SET enable_shared_storage_snapshot_in_query = 1;
        SET merge_tree_storage_snapshot_sleep_ms = 1000;
        SELECT count() FROM events WHERE (_part, _part_offset) IN (
            SELECT _part, _part_offset FROM events WHERE user_id = 2
        )" &
    PID=$!

    sleep 1.5

    $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE events FINAL"

    wait $PID
)

echo "With snapshot sharing enabled: $RESULT"

$CLICKHOUSE_CLIENT --query "DROP TABLE events"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE events
(
    user_id UInt32,
    value UInt32
) ENGINE = MergeTree()
ORDER BY user_id"

$CLICKHOUSE_CLIENT --query "INSERT INTO events VALUES (1, 100), (2, 200)"

# Run query with snapshot sharing disabled
RESULT=$(
    $CLICKHOUSE_CLIENT --query "
        SET enable_shared_storage_snapshot_in_query = 0;
        SET merge_tree_storage_snapshot_sleep_ms = 1000;
        SELECT count() FROM events WHERE (_part, _part_offset) IN (
            SELECT _part, _part_offset FROM events WHERE user_id = 2
        )" &
    PID=$!

    sleep 1.5

    $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE events FINAL"

    wait $PID
)

echo "With snapshot sharing disabled: $RESULT"

$CLICKHOUSE_CLIENT --query "DROP TABLE events"
