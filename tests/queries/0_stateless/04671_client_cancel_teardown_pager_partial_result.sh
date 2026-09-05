#!/usr/bin/env bash
# Tags: no-fasttest

# Once the result has been received, the client can still wait in the query teardown - most visibly
# for the pager to finish. There is no receive loop left there, so a first Ctrl+C can no longer be
# turned into a stage-one `Cancel` for the server: with `partial_result_on_first_cancel` the signal
# budget is never exhausted and, if the teardown only looked at the "fully cancelled" state, the
# client would keep waiting for a pager that never exits until a second Ctrl+C arrives.
# A signal that arrives *during* the teardown must therefore stop it right away.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT_OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_teardown_pager.out"
CLIENT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_teardown_pager.err"

CLIENT=""

cleanup()
{
    [ -n "$CLIENT" ] && kill -9 "$CLIENT" 2>/dev/null
    wait 2>/dev/null
    rm -f "$CLIENT_OUT" "$CLIENT_ERR"
}
trap cleanup EXIT

QUERY_ID="${CLICKHOUSE_DATABASE}_cancel_teardown_pager"

# The pager never exits on its own and never reads its input, so the client is parked in the pager
# wait of the teardown as soon as the result has been formatted. The query is long enough that the
# polling below cannot miss its running state even when every poll is slow under sanitizers.
$CLICKHOUSE_CLIENT --pager 'sleep 1000' \
    --partial_result_on_first_cancel=1 --query_id="$QUERY_ID" \
    --query "SELECT sleep(1) FROM numbers(15) SETTINGS max_block_size = 1, max_threads = 1" \
    > "$CLIENT_OUT" 2> "$CLIENT_ERR" &
CLIENT=$!

# Poll over HTTP: starting a full client for every poll is too slow under sanitizers, and a poll
# that fails must stay distinguishable from a poll that returns 0.
running_count()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'" 2>/dev/null
}

# First wait until the query is visibly running, so that the transition below really means "the
# query is over and the client moved on to the teardown" and not "the query has not started yet".
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
    echo "--- client stderr ---"
    cat "$CLIENT_ERR"
    exit 0
fi

# Then wait for it to disappear: the client is now past the receive loop and inside the teardown,
# waiting for the pager. It cannot have exited, because the pager keeps running. Only an explicit 0
# counts: a failed poll must not be mistaken for the query being over, or the Ctrl+C below would be
# sent while the query is still running and would only request the partial result.
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

if [ "$in_teardown" -ne 1 ]
then
    echo "FAIL: the query did not finish"
    echo "--- client stderr ---"
    cat "$CLIENT_ERR"
    exit 0
fi

if ! kill -0 "$CLIENT" 2>/dev/null
then
    echo "FAIL: the client exited before the pager wait was reached"
    echo "--- client stderr ---"
    cat "$CLIENT_ERR"
    exit 0
fi

# A single Ctrl+C, which does not exhaust the `partial_result_on_first_cancel` signal budget.
kill -SIGINT "$CLIENT" 2>/dev/null

for _ in {0..50}
do
    kill -0 "$CLIENT" 2>/dev/null || break
    sleep 0.2
done

if kill -0 "$CLIENT" 2>/dev/null
then
    echo "FAIL: client is still waiting for the pager after the first Ctrl+C"
    kill -9 "$CLIENT" 2>/dev/null
    exit 0
fi

echo "OK: client terminated after the first Ctrl+C"
