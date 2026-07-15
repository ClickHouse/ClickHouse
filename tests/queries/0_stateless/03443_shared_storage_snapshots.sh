#!/usr/bin/env bash
# Tags: long, no-parallel, no-parallel-replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function test_snapshot_sharing()
{
    local delay=$1 && shift

    local query_settings=( "$@" )
    query_settings+=( "--merge_tree_storage_snapshot_sleep_ms=$((delay*1000))" )

    $CLICKHOUSE_CLIENT -nm --query "
    DROP TABLE IF EXISTS events;
    CREATE TABLE events
    (
        user_id UInt32,
        value UInt32
    ) ENGINE = MergeTree()
    ORDER BY user_id;

    INSERT INTO events VALUES (1, 100);
    INSERT INTO events VALUES (2, 200);
    "

    local query_id="${CLICKHOUSE_DATABASE}_${RANDOM}${RANDOM}_sharing"
    $CLICKHOUSE_CLIENT --query_id="$query_id" "${query_settings[@]}" --query "
        SELECT count() FROM events WHERE (_part, _part_offset) IN (
            SELECT _part, _part_offset FROM events WHERE user_id = 2
        )" &
    PID=$!

    local found_delay=0
    for _ in $( seq 1 $(((delay+1)*10)) ); do
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SYSTEM FLUSH LOGS text_log"
        # We need to wait until the second subquery will enter MergeTreeData::getStorageSnapshot(), and then before the artificial delay finishes inject OPTIMIZE
        if [[ $(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT count() FROM system.text_log WHERE event_date >= yesterday() AND query_id = '$query_id' AND message_format_string = 'Injecting {}ms artificial delay before taking storage snapshot' SETTINGS max_rows_to_read = 0") -eq 2 ]]; then
            found_delay=1
            break
        fi
        sleep 0.1
    done
    if [ $found_delay -eq 0 ]; then
        wait $PID
        echo "No delay has been injected (for query_id: $query_id)"
        return
    fi

    $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE events FINAL"

    # NOTE: we can additionally ensure that the between injecting the sleep and
    # OPTIMIZE query did not processed further, to add additional guarantees,
    # but it does not worth it.
    #
    # $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    # if [[ $($CLICKHOUSE_CLIENT --query "SELECT argMax(message_format_string, _part_starting_offset + _part_offset) like 'Injecting {}ms artificial delay before taking storage snapshot' FROM system.text_log WHERE event_date >= yesterday() AND query_id = '$query_id' SETTINGS max_rows_to_read = 0") -eq 0 ]]; then
    #     wait $PID
    #     echo "Query progressed before optimize completed; test result is unreliable"
    #     return
    # else
    #     wait $PID
    # fi

    # This will print either 0/1 (depends on whether snapshot had been shared or not)
    wait $PID

    $CLICKHOUSE_CLIENT --query "DROP TABLE events"
}

function test_snapshot_shared()
{
    local result
    for _ in {1..10}; do
        result=$(test_snapshot_sharing 1 --enable_shared_storage_snapshot_in_query=1)
        if [ "$result" = 1 ]; then
            echo "With snapshot sharing enabled: $result"
            return
        fi
    done

    echo "All iterations has been failed. Unable to test snapshot sharing enabled. Last result: $result"
}

function test_snapshot_not_shared()
{
    local result
    for _ in {1..10}; do
        # NOTE: when snapshot should not be shared, the test is sensitive to the merge_tree_storage_snapshot_sleep_ms
        result=$(test_snapshot_sharing 7 --enable_shared_storage_snapshot_in_query=0)
        if [ "$result" = 0 ]; then
            echo "With snapshot sharing disabled: $result"
            return
        fi
    done

    echo "All iterations has been failed. Unable to test snapshot sharing disabled. Last result: $result"
}

test_snapshot_shared
test_snapshot_not_shared
