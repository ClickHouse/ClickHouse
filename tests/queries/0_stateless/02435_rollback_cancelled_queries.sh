#!/usr/bin/env bash
# Tags: no-random-settings, no-ordinary-database, no-fasttest
# no-fasttest: The test is slow (too many small blocks)
# shellcheck disable=SC2009

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export DATA_FILE="$CLICKHOUSE_TMP/deduptest.tsv"
export TEST_MARK="02435_insert_${CLICKHOUSE_DATABASE}_"
export SESSION="02435_session_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q 'select * from numbers(1000000) format TSV' > $DATA_FILE
$CLICKHOUSE_CLIENT -q 'create table dedup_test(A Int64) Engine = MergeTree order by sin(A) partition by intDiv(A, 100000)'

function insert_data
{
    IMPLICIT=$(( RANDOM % 2 ))
    SESSION_ID="${SESSION}_$RANDOM.$RANDOM.$NUM"
    TXN_SETTINGS="session_id=$SESSION_ID&throw_on_unsupported_query_inside_transaction=0&implicit_transaction=$IMPLICIT"
    BEGIN=""
    COMMIT=""
    SETTINGS="query_id=$ID&$TXN_SETTINGS&max_insert_block_size=110000&min_insert_block_size_rows=110000"
    if [[ "$IMPLICIT" -eq 0 ]]; then
        $CLICKHOUSE_CURL -sS -d 'begin transaction' "$CLICKHOUSE_URL&$TXN_SETTINGS"
        SETTINGS="$SETTINGS&session_check=1"
        BEGIN="begin transaction;"
        COMMIT=$(echo -ne "\n\n\ncommit")
    fi

    # max_block_size=10000, so external table will contain smaller blocks that will be squashed on insert-select (more chances to catch a bug on query cancellation)
    TRASH_SETTINGS="$SETTINGS&input_format_parallel_parsing=0&max_threads=1&max_insert_threads=1&max_block_size=10000&min_insert_block_size_bytes=0"
    TYPE=$(( RANDOM % 6 ))

    if [[ "$TYPE" -eq 0 ]]; then
        $CLICKHOUSE_CURL -sS -X POST --data-binary @- "$CLICKHOUSE_URL&$SETTINGS&query=insert+into+dedup_test+format+TSV" < $DATA_FILE
    elif [[ "$TYPE" -eq 1 ]]; then
        $CLICKHOUSE_CURL -sS -X POST -H "Transfer-Encoding: chunked" --data-binary @- "$CLICKHOUSE_URL&$SETTINGS&query=insert+into+dedup_test+format+TSV" < $DATA_FILE
    elif [[ "$TYPE" -eq 2 ]]; then
        $CLICKHOUSE_CURL -sS -F 'file=@-' "$CLICKHOUSE_URL&$TRASH_SETTINGS&file_format=TSV&file_types=UInt64" -X POST --form-string 'query=insert into dedup_test select * from file' < $DATA_FILE
    else
        # client will send 1000-rows blocks, server will squash them into 110000-rows blocks (more chances to catch a bug on query cancellation)
        $CLICKHOUSE_CLIENT --stacktrace --query_id="$ID" --throw_on_unsupported_query_inside_transaction=0 --implicit_transaction="$IMPLICIT" \
            --max_block_size=1000 --max_insert_block_size=1000 -q \
            "${BEGIN}insert into dedup_test settings max_insert_block_size=110000, min_insert_block_size_rows=110000 format TSV$COMMIT" < $DATA_FILE \
            | grep -Fv "Transaction is not in RUNNING state"
    fi

    if [[ "$IMPLICIT" -eq 0 ]]; then
        $CLICKHOUSE_CURL -sS -d 'commit' "$CLICKHOUSE_URL&$TXN_SETTINGS&close_session=1" 2>&1| grep -Fav "Transaction is not in RUNNING state"
    fi
}

export -f insert_data

ID="02435_insert_init_${CLICKHOUSE_DATABASE}_$RANDOM"
insert_data 0
$CLICKHOUSE_CLIENT -q 'select count() from dedup_test'

function thread_insert
{
    # supress "Killed" messages from bash
    i=2
    while true; do
        export ID="$TEST_MARK$RANDOM-$RANDOM-$i"
        export NUM="$i"
        bash -c insert_data 2>&1| grep -Fav "Killed" | grep -Fav "SESSION_IS_LOCKED" | grep -Fav "SESSION_NOT_FOUND"
        i=$((i + 1))
    done
}

function thread_select
{
    while true; do
        $CLICKHOUSE_CLIENT --implicit_transaction=1 -q "with (select count() from dedup_test) as c select throwIf(c % 1000000 != 0, 'Expected 1000000 * N rows, got ' || toString(c)) format Null"
        sleep 0.$RANDOM;
    done
}

function thread_cancel
{
    while true; do
        SIGNAL="INT"
        if (( RANDOM % 2 )); then
            SIGNAL="KILL"
        fi
        PID=$(grep -Fa "$TEST_MARK" /proc/*/cmdline | grep -Fav grep | grep -Eoa "/proc/[0-9]*/cmdline:" | grep -Eo "[0-9]*" | head -1)
        if [ ! -z "$PID" ]; then kill -s "$SIGNAL" "$PID"; fi
        sleep 0.$RANDOM;
    done
}

export -f thread_insert;
export -f thread_select;
export -f thread_cancel;

TIMEOUT=20

timeout $TIMEOUT bash -c thread_insert &
timeout $TIMEOUT bash -c thread_select &
timeout $TIMEOUT bash -c thread_cancel 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q 'system flush logs'

ID="02435_insert_last_${CLICKHOUSE_DATABASE}_$RANDOM"
insert_data 1

$CLICKHOUSE_CLIENT --implicit_transaction=1 -q 'select throwIf(count() % 1000000 != 0 or count() = 0) from dedup_test' \
  || $CLICKHOUSE_CLIENT -q "select name, rows, active, visible, creation_tid, creation_csn from system.parts where database=currentDatabase();"

# Ensure that thread_cancel actually did something (useful when editing this test)
# We cannot check it in the CI, because sometimes it fails due to randomization
# $CLICKHOUSE_CLIENT -q "select count() > 0 from system.text_log where event_date >= yesterday() and query_id like '$TEST_MARK%' and (
#   message_format_string in ('Unexpected end of file while reading chunk header of HTTP chunked data', 'Unexpected EOF, got {} of {} bytes',
#   'Query was cancelled or a client has unexpectedly dropped the connection') or
#   message like '%Connection reset by peer%' or message like '%Broken pipe, while writing to socket%')"

wait_for_queries_to_finish 30
$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=0 -q "drop table dedup_test"
