#!/usr/bin/env bash
# Tags: no-random-settings, no-asan, no-msan, no-tsan, no-async-insert, no-debug, no-fasttest, no-replicated-database
# no-fasttest: The test runs for 40 seconds
# shellcheck disable=SC2009

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export DATA_FILE="$CLICKHOUSE_TMP/deduptest_select.tsv"
export TEST_MARK="04366_insert_${CLICKHOUSE_DATABASE}_"

# Sibling of 02434_cancel_insert_when_client_dies for the INSERT SELECT insert source. Under
# insert_deduplication_version=new_unified_hash the deduplication id is keyed by the insert source,
# so each source is covered by its own test. Here the data is uploaded over HTTP as an external
# table and inserted with INSERT ... SELECT ... ORDER BY all. The ORDER BY (with single-threaded
# reading/insertion) makes the block boundaries deterministic, which the order-sensitive unified
# hash requires to deduplicate retries. The table must stay at 500000 rows while inserts are
# repeatedly cancelled (the client is killed) and retried.
$CLICKHOUSE_CLIENT -q 'select * from numbers(500000) format TSV' > $DATA_FILE
$CLICKHOUSE_CLIENT -q "create table dedup_test(A Int64) Engine = MergeTree order by A settings non_replicated_deduplication_window=1000, merge_tree_clear_old_temporary_directories_interval_seconds = 1"

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --async_insert=0"
CLICKHOUSE_URL="${CLICKHOUSE_URL}&async_insert=0"

function insert_data
{
    local ID=$1
    # max_block_size=10000, so external table will contain smaller blocks that will be squashed on insert-select (more chances to catch a bug on query cancellation)
    TRASH_SETTINGS="query_id=$ID&input_format_parallel_parsing=0&max_threads=1&max_insert_threads=1&max_insert_block_size=110000&max_block_size=10000&min_insert_block_size_bytes=0&min_insert_block_size_rows=110000&max_insert_block_size=110000&send_logs_level=fatal"
    $CLICKHOUSE_CURL -sS -F 'file=@-' "$CLICKHOUSE_URL&$TRASH_SETTINGS&file_format=TSV&file_types=UInt64" -X POST --form-string 'query=insert into dedup_test select * from file order by all' < $DATA_FILE
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
