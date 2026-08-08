#!/usr/bin/env bash
# Tags: no-random-settings, no-asan, no-msan, no-tsan, no-async-insert, no-debug, no-fasttest, no-replicated-database
# no-fasttest: The test runs for 40 seconds
# shellcheck disable=SC2009

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export DATA_FILE="$CLICKHOUSE_TMP/deduptest.tsv"
export TEST_MARK="02434_insert_${CLICKHOUSE_DATABASE}_"

# Under insert_deduplication_version=new_unified_hash the deduplication id is keyed by the insert
# source, so this test drives a single source - direct inserts into the destination table - while
# varying only the protocol (native / HTTP / HTTP chunked). Those share the same source id and so
# cross-deduplicate; the table must stay at 500000 rows while inserts are repeatedly cancelled (the
# client is killed) and retried. The distributed and INSERT SELECT sources are covered by the
# sibling 02434_cancel_insert_when_client_dies_* tests.
$CLICKHOUSE_CLIENT -q 'select * from numbers(500000) format TSV' > $DATA_FILE
$CLICKHOUSE_CLIENT -q "create table dedup_test(A Int64) Engine = MergeTree order by A settings non_replicated_deduplication_window=1000, merge_tree_clear_old_temporary_directories_interval_seconds = 1"

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --async_insert=0"
CLICKHOUSE_URL="${CLICKHOUSE_URL}&async_insert=0"

function insert_data
{
    local ID=$1

    # send_logs_level: https://github.com/ClickHouse/ClickHouse/issues/67599
    SETTINGS="query_id=$ID&max_insert_block_size=110000&min_insert_block_size_rows=110000&send_logs_level=fatal"
    TYPE=$(( RANDOM % 3 ))

    if [[ "$TYPE" -eq 0 ]]; then
        # client will send 10000-rows blocks, server will squash them into 110000-rows blocks (more chances to catch a bug on query cancellation)
        $CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=fatal --max_block_size=10000 --max_insert_block_size=10000 --query_id="$ID" \
            -q 'insert into dedup_test settings max_insert_block_size=110000, min_insert_block_size_rows=110000 format TSV' < $DATA_FILE
    elif [[ "$TYPE" -eq 1 ]]; then
        $CLICKHOUSE_CURL -sS -X POST --data-binary @- "$CLICKHOUSE_URL&$SETTINGS&query=insert+into+dedup_test+format+TSV" < $DATA_FILE
    else
        $CLICKHOUSE_CURL -sS -X POST -H "Transfer-Encoding: chunked" --data-binary @- "$CLICKHOUSE_URL&$SETTINGS&query=insert+into+dedup_test+format+TSV" < $DATA_FILE
    fi
}

export -f insert_data

insert_data ${TEST_MARK}-${RANDOM}_first_run
$CLICKHOUSE_CLIENT -q 'select count() from dedup_test'

function thread_insert
{
    # supress "Killed" messages from bash
    i=0
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        bash -c "insert_data ${TEST_MARK}-${RANDOM}-${RANDOM}-$i" 2>&1| grep -Fav "Killed"
        i=$((i + 1))
    done
}

function thread_select
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT -q "with (select count() from dedup_test) as c select throwIf(c != 500000, 'Expected 500000 rows, got ' || toString(c)) format Null"
        sleep 0.$RANDOM;
    done
}

function thread_cancel
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        SIGNAL="INT"
        if (( RANDOM % 2 )); then
            SIGNAL="KILL"
        fi

        PID=$(grep -Fa "query_id=$TEST_MARK" /proc/*/cmdline | grep -Fav grep | grep -Fav insert_data | grep -Eoa "/proc/[0-9]*/cmdline:" | grep -Eo "[0-9]*" | head -1)
        if [ ! -z "$PID" ]; then kill -s "$SIGNAL" "$PID"; fi
        sleep 0.$RANDOM;
        sleep 0.$RANDOM;
        sleep 0.$RANDOM;
    done
}

TIMEOUT=40

thread_insert &
thread_select &
thread_cancel 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q 'select count() from dedup_test'

$CLICKHOUSE_CLIENT -q 'system flush logs text_log'

# Ensure that thread_cancel actually did something
$CLICKHOUSE_CLIENT -q "select count() > 0 from system.text_log where event_date >= yesterday() AND event_time >= now() - 600 and query_id like '$TEST_MARK%' and (
  message_format_string in ('Unexpected end of file while reading chunk header of HTTP chunked data', 'Unexpected EOF, got {} of {} bytes',
  'Query was cancelled or a client has unexpectedly dropped the connection') or
  message like '%Connection reset by peer%' or message like '%Broken pipe, while writing to socket%') SETTINGS max_rows_to_read = 0"
