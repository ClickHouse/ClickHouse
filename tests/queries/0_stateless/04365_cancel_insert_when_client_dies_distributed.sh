#!/usr/bin/env bash
# Tags: no-random-settings, no-asan, no-msan, no-tsan, no-async-insert, no-debug, no-fasttest, no-replicated-database
# no-fasttest: The test runs for 40 seconds

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export DATA_FILE="$CLICKHOUSE_TMP/deduptest_dist.tsv"
export TEST_MARK="04365_insert_${CLICKHOUSE_DATABASE}_"

# Sibling of 02434_cancel_insert_when_client_dies for the distributed insert source. Under
# insert_deduplication_version=new_unified_hash the deduplication id is keyed by the insert source,
# so each source is covered by its own test. Here inserts go through a Distributed table with
# distributed_foreground_insert=1 (synchronous forward, so the row count is immediately consistent).
# The table must stay at 500000 rows while inserts are repeatedly cancelled (the client is killed)
# and retried.
$CLICKHOUSE_CLIENT -q 'select * from numbers(500000) format TSV' > $DATA_FILE
$CLICKHOUSE_CLIENT -q "create table dedup_test(A Int64) Engine = MergeTree order by A settings non_replicated_deduplication_window=1000, merge_tree_clear_old_temporary_directories_interval_seconds = 1"
$CLICKHOUSE_CLIENT -q "create table dedup_dist(A Int64) Engine = Distributed('test_cluster_one_shard_two_replicas', currentDatabase(), dedup_test)"

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --async_insert=0"

function insert_data
{
    local ID=$1
    local CANCEL=$2
    # client will send 10000-rows blocks, server will squash them into 110000-rows blocks (more chances to catch a bug on query cancellation)
    # Feed through a FIFO this function keeps open. The first chunk dwarfs the 64KiB pipe, so the write
    # below cannot finish until the client has drained it, and while the write end stays open the client
    # never sees EOF and the server cannot finish the insert. That is what puts a cancellation inside
    # the insert instead of wherever it happened to land.
    local FIFO="$CLICKHOUSE_TMP/deduptest_dist_feed_$$"
    rm -f "$FIFO"
    mkfifo "$FIFO"
    $CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=fatal --max_block_size=10000 --max_insert_block_size=10000 --query_id="$ID" \
        -q 'insert into dedup_dist settings max_insert_block_size=110000, min_insert_block_size_rows=110000, distributed_foreground_insert=1 format TSV' \
        < "$FIFO" &
    local CLIENT_PID=$!
    # Let bash pick the descriptor: shell_config.sh points BASH_XTRACEFD at fd 3 under CI, so writing
    # rows to fd 3 would interleave trace output into the INSERT stream.
    exec {feed_fd}>"$FIFO"
    head -n 440000 "$DATA_FILE" >&${feed_fd}
    if [ -n "$CANCEL" ]; then
        local SIGNAL="INT"
        if (( RANDOM % 2 )); then
            SIGNAL="KILL"
        fi
        # Kill only once the sink has deduplicated a block of this insert, so the cancellation hits the
        # stage this test is about. The feed is still held, so the insert cannot leave that stage while
        # we look. If it is never observed, leave the insert alone rather than kill blindly.
        # Match initial_query_id, not query_id: the event is raised by the MergeTree sink of the
        # shard-level insert, which is a nested query of this Distributed one.
        # Reaching that stage at all needs the synchronous insert path (--async_insert=0 above): an
        # asynchronous insert buffers up to async_insert_max_data_size server-side before any block
        # enters a pipeline, and a chunk this size would leave nothing in flight.
        local ATTEMPT=0
        while [ $ATTEMPT -lt 40 ]
        do
            if [ "$($CLICKHOUSE_CLIENT -q "select count() > 0 from system.processes where initial_query_id = '$ID' and ProfileEvents['DuplicatedInsertedBlocks'] > 0" 2>/dev/null)" = "1" ]; then
                kill -s "$SIGNAL" "$CLIENT_PID" 2>/dev/null
                break
            fi
            ATTEMPT=$((ATTEMPT + 1))
            sleep 0.1;
        done
    fi
    # The rest of the data. A SIGINT'd client only tests its interrupt flag after its next pull, so it
    # needs one more block before EOF or it would finish the insert instead of cancelling it. A SIGKILL'd
    # client is already gone, so this write just dies on the closed pipe; its error is discarded because
    # thread_insert folds stderr into the compared stdout.
    tail -n +440001 "$DATA_FILE" 1>&${feed_fd} 2>/dev/null
    exec {feed_fd}>&-
    wait "$CLIENT_PID"
    rm -f "$FIFO"
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
        # Cancel every other insert: the ones left to finish are the retries whose blocks must be
        # deduplicated rather than duplicated.
        CANCEL=""
        if [ $((i % 2)) -eq 0 ]; then CANCEL="cancel"; fi
        bash -c "insert_data ${TEST_MARK}-${RANDOM}-${RANDOM}-$i $CANCEL" 2>&1| grep -Fav "Killed"
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

TIMEOUT=40

thread_insert &
thread_select &

wait

$CLICKHOUSE_CLIENT -q 'select count() from dedup_test'

$CLICKHOUSE_CLIENT -q 'system flush logs text_log'
$CLICKHOUSE_CLIENT -q 'system flush logs query_log'

# Ensure that the cancellations in insert_data actually did something. Native-protocol SIGINT sends a graceful
# 'Cancel' packet (connection preserved) rather than dropping the socket, so match the
# QUERY_WAS_CANCELLED_BY_CLIENT messages too; the socket-drop messages only fire on SIGKILL.
CANCELLED=$($CLICKHOUSE_CLIENT -q "select count() > 0 from system.text_log where event_date >= yesterday() AND event_time >= now() - 600 and query_id like '$TEST_MARK%' and (
  message_format_string in ('Unexpected end of file while reading chunk header of HTTP chunked data', 'Unexpected EOF, got {} of {} bytes',
  'Query was cancelled or a client has unexpectedly dropped the connection',
  'Received ''Cancel'' packet from the client, canceling the query.',
  'Packet ''Cancel'' has been received from the client, canceling the query.') or
  message like '%Connection reset by peer%' or message like '%Broken pipe, while writing to socket%') SETTINGS max_rows_to_read = 0")
echo "$CANCELLED"
if [ "$CANCELLED" != "1" ]; then
    # Distinguish "no insert was cancelled" from "one was and text_log did not keep the message".
    $CLICKHOUSE_CLIENT -q "select type, count() from system.query_log where event_date >= yesterday() and current_database = currentDatabase() and query_id like '$TEST_MARK%' group by type order by type format TSV" >&2
fi

# A cancellation that never reached the deduplicating stage does not exercise this test, so require at
# least one that did. The event is on the shard-level insert, so this must group the whole query chain.
$CLICKHOUSE_CLIENT -q "select throwIf(count() = 0, 'No cancelled insert had reached the deduplicating stage') from (
  select initial_query_id from system.query_log where event_date >= yesterday() and initial_query_id like '$TEST_MARK%'
  group by initial_query_id
  having countIf(is_initial_query and current_database = currentDatabase() and type = 'ExceptionWhileProcessing') > 0
     and sum(ProfileEvents['DuplicatedInsertedBlocks']) > 0
) SETTINGS max_rows_to_read = 0 format Null"
