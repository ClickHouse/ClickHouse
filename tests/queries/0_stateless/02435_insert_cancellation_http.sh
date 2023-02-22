#!/usr/bin/env bash
# shellcheck disable=SC2009

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export DATA_FILE="$CLICKHOUSE_TMP/deduptest.tsv"
export TEST_MARK="02435_insert_${CLICKHOUSE_DATABASE}_"

$CLICKHOUSE_CLIENT -q 'select * from numbers(5000000) format TSV' > $DATA_FILE
$CLICKHOUSE_CLIENT -q 'create table dedup_test(A Int64) Engine = MergeTree order by A settings non_replicated_deduplication_window=1000;'
$CLICKHOUSE_CURL -sS -X POST --data-binary @- "$CLICKHOUSE_URL&max_insert_block_size=100000&input_format_parallel_parsing=0&query=insert+into+dedup_test+format+TSV"  < $DATA_FILE
$CLICKHOUSE_CLIENT -q 'select count() from dedup_test'

function thread_insert
{
    # supress "Killed" messages from bash
    function wrap
    {
        if (( RANDOM % 2 )); then
            $CLICKHOUSE_CURL -sS -X POST --data-binary @- "$CLICKHOUSE_URL&query_id=$ID&max_insert_block_size=100000&input_format_parallel_parsing=0&query=insert+into+dedup_test+format+TSV"  < $DATA_FILE
        else
            $CLICKHOUSE_CURL -sS -X POST -H "Transfer-Encoding: chunked" --data-binary @- "$CLICKHOUSE_URL&query_id=$ID&max_insert_block_size=100000&input_format_parallel_parsing=0&query=insert+into+dedup_test+format+TSV"  < $DATA_FILE
        fi
    }
    export -f wrap
    while true; do
        export ID="$TEST_MARK$RANDOM"
        bash -c wrap 2>&1| grep -Fav "Killed"
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
        PID=$(ps -ef | grep "$TEST_MARK" | grep -v grep | awk '{print $2}')
        if [ ! -z "$PID" ]; then kill -s "$SIGNAL" "$PID" || echo "$PID"; fi
        sleep 0.$RANDOM;
        sleep 0.$RANDOM;
        sleep 0.$RANDOM;
    done
}

export -f thread_insert;
export -f thread_select;
export -f thread_cancel;

TIMEOUT=30

timeout $TIMEOUT bash -c thread_insert &
timeout $TIMEOUT bash -c thread_select &
timeout $TIMEOUT bash -c thread_cancel &

wait

$CLICKHOUSE_CLIENT -q 'select count() from dedup_test'
