#!/usr/bin/env bash
# Tags: no-fasttest, no-old-analyzer
# no-old-analyzer: make_distributed_plan requires the analyzer.
# Checks that cancelling a distributed-plan query terminates it promptly, for both exchange kinds:
# Streaming exercises waking worker tasks blocked on in-memory exchanges, Persisted additionally
# exercises interrupting the stage-dependency wait that runs under the executor mutex.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_dp_cancel (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_dp_cancel SELECT number FROM numbers(100000)"

function run_cancel_check()
{
    local exchange_kind=$1
    local extra_settings=""
    if [ "$exchange_kind" != "default" ]; then
        extra_settings=", distributed_plan_force_exchange_kind = '$exchange_kind'"
    fi
    local query_id="distributed_plan_cancel_${exchange_kind}_${CLICKHOUSE_DATABASE}"
    local client_log="${CLICKHOUSE_TMP}/04335_client_${exchange_kind}_${CLICKHOUSE_DATABASE}.err"

    echo "exchange kind: $exchange_kind"

    # sleepEachRow is capped at 3 seconds of sleep per block, so bound the block size: read blocks
    # are granule-sized at minimum (hence index_granularity = 1000 above) and max_block_size is
    # pinned against randomization. 1000 rows x 0.001s = 1s per block, ~100s natural query duration.
    $CLICKHOUSE_CLIENT --query-id="$query_id" -q "
        SELECT x, count() FROM t_dp_cancel WHERE NOT sleepEachRow(0.001) GROUP BY x FORMAT Null
        SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1, distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
            distributed_plan_max_rows_to_broadcast = 0, max_rows_to_group_by = 0, max_execution_time = 300,
            max_block_size = 1000$extra_settings
    " 2> "$client_log" &
    local query_pid=$!

    local started=0
    for _ in {1..300}; do
        started=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id'")
        [ "$started" -ge 1 ] && break
        sleep 0.1
    done
    # Fail loudly if the query never ran: otherwise the KILL below has nothing to kill and the test
    # passes without exercising cancellation. The client log explains why the query died early.
    echo "query started: $started"
    if [ "$started" -eq 0 ]; then
        cat "$client_log"
    fi

    # SYNC waits for the query to actually terminate; bound it so a cancellation hang fails the
    # test instead of hanging the runner.
    timeout 60 $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null" || echo "KILL timed out"

    # Bound the wait for the client too: with a broken cancellation path the client never exits on
    # its own (the query may ignore max_execution_time as well), and an unbounded wait would hang
    # the test runner instead of failing the test.
    local client_exited=0
    for _ in {1..600}; do
        if ! kill -0 "$query_pid" 2>/dev/null; then
            client_exited=1
            break
        fi
        sleep 0.1
    done
    if [ "$client_exited" -eq 0 ]; then
        echo "client did not exit"
        kill -9 "$query_pid" 2>/dev/null
    fi
    wait "$query_pid" 2>/dev/null || true

    local lingering=1
    for _ in {1..300}; do
        lingering=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id'")
        [ "$lingering" -eq 0 ] && break
        sleep 0.1
    done
    echo "lingering queries: $lingering"
}

run_cancel_check "default"
run_cancel_check "Persisted"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_dp_cancel"
