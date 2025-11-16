#!/usr/bin/env bash
# Tags: long, no-replicated-database, no-ordinary-database

# shellcheck disable=SC2015
# shellcheck disable=SC2119

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

set -eu

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS src";
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS dst";
$CLICKHOUSE_CLIENT --query "CREATE TABLE src (n UInt64, type UInt8) ENGINE=MergeTree ORDER BY type SETTINGS old_parts_lifetime=0";
$CLICKHOUSE_CLIENT --query "CREATE TABLE dst (n UInt64, type UInt8) ENGINE=MergeTree ORDER BY type SETTINGS old_parts_lifetime=0";

function thread_insert()
{
    set -eu
    val=1
    while true; do
        $CLICKHOUSE_CLIENT --query "
        BEGIN TRANSACTION;
        INSERT INTO src VALUES /* ($val, 1) */ ($val, 1);
        INSERT INTO src VALUES /* ($val, 2) */ ($val, 2);
        COMMIT;"
        val=$((val+1))
        sleep 0.$RANDOM;
    done
}

function is_tx_aborted_with()
{
    grep_args=""
    for pattern in "${@}"; do
      grep_args="$grep_args -Fe $pattern"
    done

    grep $grep_args >/dev/null
}

function is_tx_failed()
{
    grep -Fe 'DB::Exception:' > /dev/null
}

function is_tx_ok()
{
    is_tx_failed && return 1
}

# NOTE
# ALTER PARTITION query stops merges,
# but parts could be deleted (SERIALIZATION_ERROR) if some merge was assigned (and committed) between BEGIN and ALTER.
function thread_partition_src_to_dst()
{
    set -eu
    count=0
    sum=0
    for i in {1..20}; do
        session_id="_src_to_dst_$i"
        session_id_debug="_src_to_dst_debug_$i"

        tx $session_id "BEGIN TRANSACTION"
        tx_id=$(tx $session_id "select transactionID().1" | awk '{print $2}')

        tx $session_id "INSERT INTO src VALUES /* ($i, 3) */ ($i, 3)"
        tx $session_id "INSERT INTO dst SELECT * FROM src"

        output=$(tx $session_id "ALTER TABLE src DROP PARTITION ID 'all'" ||:)
        if echo "$output" | is_tx_aborted_with "SERIALIZATION_ERROR" "PART_IS_TEMPORARILY_LOCKED" "PART_IS_TEMPORARILY_LOCKED"
        then
            tx $session_id "ROLLBACK"
            continue
        fi

        if echo "$output" | is_tx_failed
        then
            echo "thread_partition_src_to_dst tx_id: $tx_id session_id: $session_id" >&2
            echo "drop part has failed with unexpected status" >&2
            echo -e "output:\n $output" >&2
            return 1
        fi

        tx $session_id "SET throw_on_unsupported_query_inside_transaction=0"

        trace_output=""
        output=$(tx $session_id "select transactionID()")
        trace_output="$trace_output $output\n"

        tx $session_id_debug "begin transaction"
        tx $session_id_debug "set transaction snapshot 3"
        output=$(tx $session_id_debug "select 'src_to_dst', $i, 'src', type, n, _part from src order by type, n")
        trace_output="$trace_output $output\n"
        output=$(tx $session_id_debug "select 'src_to_dst', $i, 'dst', type, n, _part from dst order by type, n")
        trace_output="$trace_output $output\n"
        tx $session_id_debug "commit"

        output=$(tx $session_id "SELECT throwIf((SELECT (count(), sum(n)) FROM merge(currentDatabase(), '') WHERE type=3) != ($count + 1, $sum + $i)) FORMAT Null" ||:)
        if echo "$output" | is_tx_aborted_with "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"
        then
            echo "thread_partition_src_to_dst tx_id: $tx_id session_id: $session_id" >&2
            echo "select throwIf has failed with FUNCTION_THROW_IF_VALUE_IS_NON_ZERO" >&2
            echo -e "trace_output:\n $trace_output" >&2
            echo -e "output:\n $output" >&2
            return 1
        fi

        if echo "$output" | is_tx_failed
        then
            echo "thread_partition_src_to_dst tx_id: $tx_id session_id: $session_id" >&2
            echo "select throwIf has failed with unexpected status" >&2
            echo -e "trace_output:\n $trace_output" >&2
            echo -e "output:\n $output" >&2
            return 1
        fi

        tx $session_id "COMMIT"

        count=$((count + 1))
        sum=$((sum + i))

    done
}

function thread_partition_dst_to_src()
{
    set -eu
    i=0
    while (( i <= 20 )); do
        session_id="_dst_to_src_$i"
        session_id_debug="_dst_to_src_debug_$i"

        tx $session_id "SYSTEM STOP MERGES dst"
        tx $session_id "ALTER TABLE dst DROP PARTITION ID 'nonexistent';"
        tx $session_id "SYSTEM SYNC TRANSACTION LOG"

        tx $session_id "BEGIN TRANSACTION"
        tx_id=$(tx $session_id "select transactionID().1" | awk '{print $2}')

        tx $session_id "INSERT INTO dst VALUES /* ($i, 4) */ ($i, 4)"
        tx $session_id "INSERT INTO src SELECT * FROM dst"

        output=$(tx $session_id "ALTER TABLE dst DROP PARTITION ID 'all'" ||:)
        if echo "$output" | is_tx_aborted_with "PART_IS_TEMPORARILY_LOCKED"
        then
            # this is legit case, just retry
            tx $session_id "ROLLBACK"
            continue
        fi

        if echo "$output" | is_tx_failed
        then
            echo "thread_partition_dst_to_src tx_id: $tx_id session_id: $session_id" >&2
            echo "drop part has failed with unexpected status" >&2
            echo "output $output" >&2
            return 1
        fi

        tx $session_id "SET throw_on_unsupported_query_inside_transaction=0"
        tx $session_id "SYSTEM START MERGES dst"

        trace_output=""
        output=$(tx $session_id "select transactionID()")
        trace_output="$trace_output $output"

        tx $session_id_debug "begin transaction"
        tx $session_id_debug "set transaction snapshot 3"
        output=$(tx $session_id_debug "select 'dst_to_src', $i, 'src', type, n, _part from src order by type, n")
        trace_output="$trace_output $output"
        output=$(tx $session_id_debug "select 'dst_to_src', $i, 'dst', type, n, _part from dst order by type, n")
        trace_output="$trace_output $output"
        tx $session_id_debug "commit"

        output=$(tx $session_id "SELECT throwIf((SELECT (count(), sum(n)) FROM merge(currentDatabase(), '') WHERE type=4) != (toUInt8($i/2 + 1), (select sum(number) from numbers(1, $i) where number % 2 or number=$i))) FORMAT Null" ||:)
        if echo "$output" | is_tx_aborted_with "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"
        then
            echo "thread_partition_dst_to_src tx_id: $tx_id session_id: $session_id" >&2
            echo "select throwIf has failed with FUNCTION_THROW_IF_VALUE_IS_NON_ZERO" >&2
            echo -e "trace_output:\n $trace_output" >&2
            echo -e "output:\n $output" >&2
            return 1
        fi

        if echo "$output" | is_tx_failed
        then
            echo "thread_partition_dst_to_src tx_id: $tx_id session_id: $session_id" >&2
            echo "SELECT throwIf has failed with unexpected status" >&2
            echo -e "trace_output:\n $trace_output" >&2
            echo -e "output:\n $output" >&2
            return 1
        fi

        action="ROLLBACK"
        if (( i % 2 )); then
            action="COMMIT"
        fi

        tx $session_id "$action"

        i=$((i + 1))
    done
}

function thread_select()
{
    set -eu
    while true; do
        output=$(
        $CLICKHOUSE_CLIENT --query "
        BEGIN TRANSACTION;
        -- no duplicates
        SELECT type, throwIf(count(n) != countDistinct(n)) FROM src GROUP BY type FORMAT Null;
        SELECT type, throwIf(count(n) != countDistinct(n)) FROM dst GROUP BY type FORMAT Null;
        -- rows inserted by thread_insert moved together
        SET throw_on_unsupported_query_inside_transaction=0;

        SELECT _table, throwIf(arraySort(groupArrayIf(n, type=1)) != arraySort(groupArrayIf(n, type=2))) FROM merge(currentDatabase(), '') GROUP BY _table FORMAT Null;

        -- all rows are inserted in insert_thread
        SELECT type, throwIf(count(n) != max(n)), throwIf(sum(n) != max(n)*(max(n)+1)/2) FROM merge(currentDatabase(), '') WHERE type IN (1, 2) GROUP BY type ORDER BY type FORMAT Null;
        COMMIT;" 2>&1 ||:)

        echo "$output" | grep -F "Received from " > /dev/null && echo "$output">&2 && return 1
    done
}

thread_insert & PID_1=$!
thread_select & PID_2=$!

thread_partition_src_to_dst & PID_3=$!
thread_partition_dst_to_src & PID_4=$!
wait $PID_3
wait $PID_4

kill -TERM $PID_1
kill -TERM $PID_2
wait ||:

wait_for_queries_to_finish 40

$CLICKHOUSE_CLIENT --implicit_transaction=1 --throw_on_unsupported_query_inside_transaction=0 -q "SELECT type, count(n) = countDistinct(n) FROM merge(currentDatabase(), '') GROUP BY type ORDER BY type"
$CLICKHOUSE_CLIENT --implicit_transaction=1 --throw_on_unsupported_query_inside_transaction=0 -q "SELECT DISTINCT arraySort(groupArrayIf(n, type=1)) = arraySort(groupArrayIf(n, type=2)) FROM merge(currentDatabase(), '') GROUP BY _table ORDER BY _table"
$CLICKHOUSE_CLIENT --implicit_transaction=1 --throw_on_unsupported_query_inside_transaction=0 -q "SELECT count(n), sum(n) FROM merge(currentDatabase(), '') WHERE type=4"
$CLICKHOUSE_CLIENT --implicit_transaction=1 --throw_on_unsupported_query_inside_transaction=0 -q "SELECT type, count(n) == max(n), sum(n) == max(n)*(max(n)+1)/2 FROM merge(currentDatabase(), '') WHERE type IN (1, 2) GROUP BY type ORDER BY type"

$CLICKHOUSE_CLIENT --query "DROP TABLE src";
$CLICKHOUSE_CLIENT --query "DROP TABLE dst";
