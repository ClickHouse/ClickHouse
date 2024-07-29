#!/usr/bin/env bash
# Tags: long, no-parallel, no-ordinary-database
# Test is too heavy, avoid parallel run in Flaky Check
# shellcheck disable=SC2119

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -ue

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS src";
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS dst";
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS mv";
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS tmp";

$CLICKHOUSE_CLIENT --query "CREATE TABLE src (n Int32, m Int32, CONSTRAINT c CHECK xxHash32(n+m) % 8 != 0) ENGINE=MergeTree ORDER BY n PARTITION BY 0 < n SETTINGS old_parts_lifetime=0";
$CLICKHOUSE_CLIENT --query "CREATE TABLE dst (nm Int32, CONSTRAINT c CHECK xxHash32(nm) % 8 != 0) ENGINE=MergeTree ORDER BY nm SETTINGS old_parts_lifetime=0";
$CLICKHOUSE_CLIENT --query "CREATE MATERIALIZED VIEW mv TO dst (nm Int32) AS SELECT n*m AS nm FROM src";

$CLICKHOUSE_CLIENT --query "CREATE TABLE tmp (x UInt32, nm Int32) ENGINE=MergeTree ORDER BY (x, nm) SETTINGS old_parts_lifetime=0"

$CLICKHOUSE_CLIENT --query "INSERT INTO src VALUES (0, 0)"

function get_now()
{
    date +%s
}

is_pid_exist()
{
    local pid=$1
    ps -p $pid > /dev/null
}

function run_until_deadline_and_at_least_times()
{
    set -e

    local deadline=$1; shift
    local min_iterations=$1; shift
    local function_to_run=$1; shift

    local started_time
    started_time=$(get_now)
    local i=0

    while true
    do
        $function_to_run $i "$@"

        [[ $(get_now) -lt $deadline ]] || break

        i=$(($i + 1))
    done

    [[ $i -gt $min_iterations ]] || echo "$i/$min_iterations : not enough iterations of $function_to_run has been made from $started_time until $deadline" >&2
}

function insert_commit_action()
{
    set -e

    local i=$1; shift
    local tag=$1; shift

    # some transactions will fail due to constraint
    $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
        INSERT INTO src VALUES /* ($i, $tag) */ ($i, $tag);
        SELECT throwIf((SELECT sum(nm) FROM mv) != $(($i * $tag))) /* ($i, $tag) */ FORMAT Null;
        INSERT INTO src VALUES /* (-$i, $tag) */ (-$i, $tag);
        COMMIT;
    " 2>&1 \
    | grep -Fv "is violated at row" | grep -Fv "Transaction is not in RUNNING state" | grep -F "Received from " ||:
}


function insert_rollback_action()
{
    set -e

    local i=$1; shift
    local tag=$1; shift

    $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
        INSERT INTO src VALUES /* (42, $tag) */ (42, $tag);
        SELECT throwIf((SELECT count() FROM src WHERE n=42 AND m=$tag) != 1) FORMAT Null;
        ROLLBACK;"
}

# make merges more aggressive
function optimize_action()
{
    set -e

    optimize_query="OPTIMIZE TABLE src"
    partition_id=$(( RANDOM % 2 ))
    if (( RANDOM % 2 )); then
        optimize_query="OPTIMIZE TABLE dst"
        partition_id="all"
    fi
    if (( RANDOM % 2 )); then
        optimize_query="$optimize_query PARTITION ID '$partition_id'"
    fi
    if (( RANDOM % 2 )); then
        optimize_query="$optimize_query FINAL"
    fi
    action="COMMIT"
    if (( RANDOM % 4 )); then
        action="ROLLBACK"
    fi

    $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
            $optimize_query;
        $action;
    " 2>&1 \
    | grep -Fv "already exists, but it will be deleted soon" | grep -F "Received from " ||:

    sleep 0.$RANDOM;
}

function select_action()
{
    set -e

    $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
        SELECT throwIf((SELECT (sum(n), count() % 2) FROM src) != (0, 1)) FORMAT Null;
        SELECT throwIf((SELECT (sum(nm), count() % 2) FROM mv) != (0, 1)) FORMAT Null;
        SELECT throwIf((SELECT (sum(nm), count() % 2) FROM dst) != (0, 1)) FORMAT Null;
        SELECT throwIf((SELECT arraySort(groupArray(nm)) FROM mv) != (SELECT arraySort(groupArray(nm)) FROM dst)) FORMAT Null;
        SELECT throwIf((SELECT arraySort(groupArray(nm)) FROM mv) != (SELECT arraySort(groupArray(n*m)) FROM src)) FORMAT Null;
        COMMIT;"
}

function select_insert_action()
{
    set -e

    $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
        SELECT throwIf((SELECT count() FROM tmp) != 0) FORMAT Null;
        INSERT INTO tmp SELECT 1, n*m FROM src;
        INSERT INTO tmp SELECT 2, nm FROM mv;
        INSERT INTO tmp SELECT 3, nm FROM dst;
        INSERT INTO tmp SELECT 4, (*,).1 FROM (SELECT n*m FROM src UNION ALL SELECT nm FROM mv UNION ALL SELECT nm FROM dst);
        SELECT throwIf((SELECT countDistinct(x) FROM tmp) != 4) FORMAT Null;

        -- now check that all results are the same
        SELECT throwIf(1 != (SELECT countDistinct(arr) FROM (SELECT x, arraySort(groupArray(nm)) AS arr FROM tmp WHERE x!=4 GROUP BY x))) FORMAT Null;
        SELECT throwIf((SELECT count(), sum(nm) FROM tmp WHERE x=4) != (SELECT count(), sum(nm) FROM tmp WHERE x!=4)) FORMAT Null;
        ROLLBACK;"
}

MAIN_TIME_PART=400
SECOND_TIME_PART=30
WAIT_FINISH=60
LAST_TIME_GAP=10

if [[ $((MAIN_TIME_PART + SECOND_TIME_PART + WAIT_FINISH + LAST_TIME_GAP)) -ge  600 ]]; then
    echo "time sttings are wrong" 2>&1
    exit 1
fi

START_TIME=$(get_now)
STOP_TIME=$((START_TIME + MAIN_TIME_PART))
SECOND_STOP_TIME=$((STOP_TIME + SECOND_TIME_PART))
MIN_ITERATIONS=20

run_until_deadline_and_at_least_times $STOP_TIME $MIN_ITERATIONS insert_commit_action 1   & PID_1=$!
run_until_deadline_and_at_least_times $STOP_TIME $MIN_ITERATIONS insert_commit_action 2   & PID_2=$!
run_until_deadline_and_at_least_times $STOP_TIME $MIN_ITERATIONS insert_rollback_action 3 & PID_3=$!

run_until_deadline_and_at_least_times $SECOND_STOP_TIME $MIN_ITERATIONS optimize_action      & PID_4=$!
run_until_deadline_and_at_least_times $SECOND_STOP_TIME $MIN_ITERATIONS select_action        & PID_5=$!
run_until_deadline_and_at_least_times $SECOND_STOP_TIME $MIN_ITERATIONS select_insert_action & PID_6=$!
sleep 0.$RANDOM
run_until_deadline_and_at_least_times $SECOND_STOP_TIME $MIN_ITERATIONS select_action        & PID_7=$!
run_until_deadline_and_at_least_times $SECOND_STOP_TIME $MIN_ITERATIONS select_insert_action & PID_8=$!

wait $PID_1 || echo "insert_commit_action has failed with status $?" 2>&1
wait $PID_2 || echo "second insert_commit_action has failed with status $?" 2>&1
wait $PID_3 || echo "insert_rollback_action has failed with status $?" 2>&1

is_pid_exist $PID_4 || echo "optimize_action is not running" 2>&1
is_pid_exist $PID_5 || echo "select_action is not running" 2>&1
is_pid_exist $PID_6 || echo "select_insert_action is not running" 2>&1
is_pid_exist $PID_7 || echo "second select_action is not running" 2>&1
is_pid_exist $PID_8 || echo "second select_insert_action is not running" 2>&1

wait $PID_4 || echo "optimize_action has failed with status $?" 2>&1
wait $PID_5 || echo "select_action has failed with status $?" 2>&1
wait $PID_6 || echo "select_insert_action has failed with status $?" 2>&1
wait $PID_7 || echo "second select_action has failed with status $?" 2>&1
wait $PID_8 || echo "second select_insert_action has failed with status $?" 2>&1

wait_for_queries_to_finish $WAIT_FINISH

$CLICKHOUSE_CLIENT --multiquery --query "
    BEGIN TRANSACTION;
        SELECT throwIf((SELECT (sum(n), count() % 2) FROM src) != (0, 1)) FORMAT Null;
        SELECT throwIf((SELECT (sum(nm), count() % 2) FROM mv) != (0, 1)) FORMAT Null;
        SELECT throwIf((SELECT (sum(nm), count() % 2) FROM dst) != (0, 1)) FORMAT Null;
        SELECT throwIf((SELECT arraySort(groupArray(nm)) FROM mv) != (SELECT arraySort(groupArray(nm)) FROM dst)) FORMAT Null;
        SELECT throwIf((SELECT arraySort(groupArray(nm)) FROM mv) != (SELECT arraySort(groupArray(n*m)) FROM src)) FORMAT Null;
    COMMIT;
"

$CLICKHOUSE_CLIENT --multiquery --query  "
    SELECT throwIf((SELECT (sum(n), count() % 2) FROM src) != (0, 1)) FORMAT Null;
    SELECT throwIf((SELECT (sum(nm), count() % 2) FROM mv) != (0, 1)) FORMAT Null;
    SELECT throwIf((SELECT (sum(nm), count() % 2) FROM dst) != (0, 1)) FORMAT Null;
    SELECT throwIf((SELECT arraySort(groupArray(nm)) FROM mv) != (SELECT arraySort(groupArray(nm)) FROM dst)) FORMAT Null;
    SELECT throwIf((SELECT arraySort(groupArray(nm)) FROM mv) != (SELECT arraySort(groupArray(n*m)) FROM src)) FORMAT Null;
"

$CLICKHOUSE_CLIENT --query "DROP TABLE src";
$CLICKHOUSE_CLIENT --query "DROP TABLE dst";
$CLICKHOUSE_CLIENT --query "DROP TABLE mv";
$CLICKHOUSE_CLIENT --query "DROP TABLE tmp";
