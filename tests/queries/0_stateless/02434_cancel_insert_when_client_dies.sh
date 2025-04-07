#!/usr/bin/env bash
# Tags: no-random-settings, no-asan, no-msan, no-tsan, no-debug, no-fasttest
# no-fasttest: The test runs for 40 seconds
# shellcheck disable=SC2009

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export DATA_FILE="$CLICKHOUSE_TMP/deduptest.tsv"
export TEST_MARK="02434_insert_${CLICKHOUSE_DATABASE}_"

$CLICKHOUSE_CLIENT -q 'select * from numbers(5000000) format TSV' > $DATA_FILE
$CLICKHOUSE_CLIENT -q "create table dedup_test(A Int64) Engine = MergeTree order by A settings non_replicated_deduplication_window=1000, merge_tree_clear_old_temporary_directories_interval_seconds = 1;"
$CLICKHOUSE_CLIENT -q "create table dedup_dist(A Int64) Engine = Distributed('test_cluster_one_shard_two_replicas', currentDatabase(), dedup_test)"

function insert_data
{
    # send_logs_level: https://github.com/ClickHouse/ClickHouse/issues/67599
    SETTINGS="query_id=$ID&max_insert_block_size=110000&min_insert_block_size_rows=110000&send_logs_level=fatal"
    # max_block_size=10000, so external table will contain smaller blocks that will be squashed on insert-select (more chances to catch a bug on query cancellation)
    TRASH_SETTINGS="query_id=$ID&input_format_parallel_parsing=0&max_threads=1&max_insert_threads=1&max_insert_block_size=110000&max_block_size=10000&min_insert_block_size_bytes=0&min_insert_block_size_rows=110000&max_insert_block_size=110000"
    TYPE=$(( RANDOM % 5 ))

    if [[ "$TYPE" -eq 0 ]]; then
        # client will send 10000-rows blocks, server will squash them into 110000-rows blocks (more chances to catch a bug on query cancellation)
        $CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=fatal --max_block_size=10000 --max_insert_block_size=10000 --query_id="$ID" \
            -q 'insert into dedup_test settings max_insert_block_size=110000, min_insert_block_size_rows=110000 format TSV' < $DATA_FILE
    elif [[ "$TYPE" -eq 1 ]]; then
        $CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=fatal --max_block_size=10000 --max_insert_block_size=10000 --query_id="$ID" --prefer_localhost_replica="$(( RANDOM % 2))" \
            -q 'insert into dedup_dist settings max_insert_block_size=110000, min_insert_block_size_rows=110000 format TSV' < $DATA_FILE
    elif [[ "$TYPE" -eq 2 ]]; then
        $CLICKHOUSE_CURL -sS -X POST --data-binary @- "$CLICKHOUSE_URL&$SETTINGS&query=insert+into+dedup_test+format+TSV" < $DATA_FILE
    elif [[ "$TYPE" -eq 3 ]]; then
        $CLICKHOUSE_CURL -sS -X POST -H "Transfer-Encoding: chunked" --data-binary @- "$CLICKHOUSE_URL&$SETTINGS&query=insert+into+dedup_test+format+TSV" < $DATA_FILE
    else
        $CLICKHOUSE_CURL -sS -F 'file=@-' "$CLICKHOUSE_URL&$TRASH_SETTINGS&file_format=TSV&file_types=UInt64" -X POST --form-string 'query=insert into dedup_test select * from file' < $DATA_FILE
    fi
}

export -f insert_data

ID="02434_insert_init_${CLICKHOUSE_DATABASE}_$RANDOM"
insert_data
$CLICKHOUSE_CLIENT -q "system flush distributed dedup_dist"
$CLICKHOUSE_CLIENT -q 'select count() from dedup_test'

function thread_insert
{
    # supress "Killed" messages from bash
    i=0
    while true; do
        export ID="$TEST_MARK$RANDOM-$RANDOM-$i"
        bash -c insert_data 2>&1| grep -Fav "Killed"
        i=$((i + 1))
    done
}

function thread_select
{
    while true; do
        $CLICKHOUSE_CLIENT -q "with (select count() from dedup_test) as c select throwIf(c != 5000000, 'Expected 5000000 rows, got ' || toString(c)) format Null"
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
        sleep 0.$RANDOM;
        sleep 0.$RANDOM;
    done
}

export -f thread_insert;
export -f thread_select;
export -f thread_cancel;

TIMEOUT=40

timeout $TIMEOUT bash -c thread_insert &
timeout $TIMEOUT bash -c thread_select &
timeout $TIMEOUT bash -c thread_cancel 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q 'select count() from dedup_test'

$CLICKHOUSE_CLIENT -q 'system flush logs'

# Ensure that thread_cancel actually did something
$CLICKHOUSE_CLIENT -q "select count() > 0 from system.text_log where event_date >= yesterday() and query_id like '$TEST_MARK%' and (
  message_format_string in ('Unexpected end of file while reading chunk header of HTTP chunked data', 'Unexpected EOF, got {} of {} bytes',
  'Query was cancelled or a client has unexpectedly dropped the connection') or
  message like '%Connection reset by peer%' or message like '%Broken pipe, while writing to socket%') SETTINGS max_rows_to_read = 0"
