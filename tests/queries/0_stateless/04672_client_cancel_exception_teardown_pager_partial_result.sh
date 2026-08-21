#!/usr/bin/env bash
# Tags: no-fasttest

# Exercise the teardown entered by onReceiveExceptionFromServer(), rather than the normal
# EndOfStream teardown in 04671. The pager does not consume its input, so exception cleanup has
# to finish the formatter and then wait for the pager. With partial_result_on_first_cancel, the
# first Ctrl+C received anywhere in this cleanup must terminate the client; it must not be folded
# into resetOutput()'s teardown baseline and require a second signal.
# See https://github.com/ClickHouse/ClickHouse/pull/108078

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT_OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_exception_teardown_pager.out"
CLIENT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_exception_teardown_pager.err"
CLIENT=""

cleanup()
{
    [ -n "$CLIENT" ] && kill -9 "$CLIENT" 2>/dev/null
    wait 2>/dev/null
    rm -f "$CLIENT_OUT" "$CLIENT_ERR"
}
trap cleanup EXIT

QUERY_ID="${CLICKHOUSE_DATABASE}_cancel_exception_teardown_pager"

# The server sends the result header, then fails before the first data block with
# `DEADLOCK_AVOIDED`. `sleep 1000` neither reads nor exits, so after that exception the client
# remains in the exception-reset teardown until the signal below releases it.
# The `sleep` in the subquery only delays the first row: without it the query throws within
# milliseconds of starting and is never observable in `system.processes`, so the wait for the
# running state below would time out instead of catching the query.
$CLICKHOUSE_CLIENT --pager 'sleep 1000' --partial_result_on_first_cancel=1 --query_id="$QUERY_ID" \
    --query "SELECT number, throwIf(number = 0, 'injected deadlock', toInt32(473))
             FROM (SELECT number FROM numbers(20) WHERE NOT sleep(3))
             SETTINGS allow_custom_error_code_in_throwif = 1, max_block_size = 1, max_threads = 1" \
    > "$CLIENT_OUT" 2> "$CLIENT_ERR" &
CLIENT=$!

running_count()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'" 2>/dev/null
}

started=0
for _ in {0..240}
do
    if [ "$(running_count)" = "1" ]
    then
        started=1
        break
    fi
    kill -0 "$CLIENT" 2>/dev/null || break
    sleep 0.5
done

if [ "$started" -ne 1 ]
then
    echo "FAIL: the query did not reach the running state"
    cat "$CLIENT_ERR"
    exit 0
fi

# The server exception has removed the query while the client remains in resetOutput() waiting
# for the pager. Wait for that transition so this is not a stage-one query cancellation.
in_teardown=0
for _ in {0..240}
do
    if [ "$(running_count)" = "0" ]
    then
        in_teardown=1
        break
    fi
    sleep 0.5
done

if [ "$in_teardown" -ne 1 ] || ! kill -0 "$CLIENT" 2>/dev/null
then
    echo "FAIL: the client did not reach exception teardown"
    cat "$CLIENT_ERR"
    exit 0
fi

kill -SIGINT "$CLIENT" 2>/dev/null

for _ in {0..50}
do
    kill -0 "$CLIENT" 2>/dev/null || break
    sleep 0.2
done

if kill -0 "$CLIENT" 2>/dev/null
then
    echo "FAIL: client is still waiting after the first Ctrl+C in exception teardown"
    kill -9 "$CLIENT" 2>/dev/null
else
    echo "OK: client terminated after the first Ctrl+C in exception teardown"
fi

if grep -Fq 'will retry' "$CLIENT_ERR"
then
    echo "FAIL: client retried DEADLOCK_AVOIDED after Ctrl+C during teardown"
fi
