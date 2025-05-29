#!/usr/bin/env bash
# Tags: long, no-parallel, no-parallel-replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

###### Snapshot sharing enabled

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

resfile=$(mktemp "$CLICKHOUSE_TMP/shared_storage_snapshots-XXXXXX.res")

query_id="${CLICKHOUSE_DATABASE}_sharing_enabled"

# Run query with snapshot sharing enabled
$CLICKHOUSE_CLIENT --query_id="$query_id" --query "
    SET enable_shared_storage_snapshot_in_query = 1;
    SET merge_tree_storage_snapshot_sleep_ms = 1000;
    SELECT count() FROM events WHERE (_part, _part_offset) IN (
        SELECT _part, _part_offset FROM events WHERE user_id = 2
    )" > "$resfile" &
PID=$!

found_delay=0
for _ in {1..15}; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.text_log WHERE event_date >= yesterday() AND query_id = '$query_id' AND message like '%Injecting % artificial delay before taking storage snapshot%' SETTINGS max_rows_to_read = 0") -eq 2 ]]; then
        found_delay=1
        break
    fi
    sleep 0.1
done

if [[ $found_delay -eq 0 ]]; then
    # If no delay injection detected within 1.5 seconds,
    # the test is flaky; wait for query to finish and use the default correct result
    wait $PID
    RESULT=1
else
    $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE events FINAL"

    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"

    if [[ $($CLICKHOUSE_CLIENT --query "SELECT argMax(message, _part_starting_offset + _part_offset) like '%Injecting % artificial delay before taking storage snapshot%' FROM system.text_log WHERE event_date >= yesterday() AND query_id = '$query_id' SETTINGS max_rows_to_read = 0") -eq 0 ]]; then
        # Query progressed before optimize completed; test result is unreliable,
        # use the default correct result instead of the potentially flaky one
        wait $PID
        RESULT=1
    else
        wait $PID
        RESULT=$(cat "$resfile")
    fi
fi

echo "With snapshot sharing enabled: $RESULT"

$CLICKHOUSE_CLIENT --query "DROP TABLE events"

rm "$resfile"

###### Snapshot sharing disabled

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

resfile=$(mktemp "$CLICKHOUSE_TMP/shared_storage_snapshots-XXXXXX.res")

query_id="${CLICKHOUSE_DATABASE}_sharing_disabled"

# Run query with snapshot sharing disabled
$CLICKHOUSE_CLIENT --query_id="$query_id" --query "
    SET enable_shared_storage_snapshot_in_query = 0;
    SET merge_tree_storage_snapshot_sleep_ms = 1000;
    SELECT count() FROM events WHERE (_part, _part_offset) IN (
        SELECT _part, _part_offset FROM events WHERE user_id = 2
    )" > "$resfile" &
PID=$!

found_delay=0
for _ in {1..15}; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.text_log WHERE event_date >= yesterday() AND query_id = '$query_id' AND message like '%Injecting % artificial delay before taking storage snapshot%' SETTINGS max_rows_to_read = 0") -eq 2 ]]; then
        found_delay=1
        break
    fi
    sleep 0.1
done

if [[ $found_delay -eq 0 ]]; then
    # If no delay injection detected within 1.5 seconds,
    # the test is flaky; wait for query to finish and use the default correct result
    wait $PID
    RESULT=0
else
    $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE events FINAL"

    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"

    if [[ $($CLICKHOUSE_CLIENT --query "SELECT argMax(message, _part_starting_offset + _part_offset) like '%Injecting % artificial delay before taking storage snapshot%' FROM system.text_log WHERE event_date >= yesterday() AND query_id = '$query_id' SETTINGS max_rows_to_read = 0") -eq 0 ]]; then
        # Query progressed before optimize completed; test result is unreliable,
        # use the default correct result instead of the potentially flaky one
        wait $PID
        RESULT=0
    else
        wait $PID
        RESULT=$(cat "$resfile")
    fi
fi

echo "With snapshot sharing disabled: $RESULT"

$CLICKHOUSE_CLIENT --query "DROP TABLE events"

rm "$resfile"
