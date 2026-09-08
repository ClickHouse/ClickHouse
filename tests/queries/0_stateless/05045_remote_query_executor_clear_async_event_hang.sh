#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs libfiu for failpoints
# Tag no-parallel: failpoints are global for the whole process
#
# Regression test for a hang in `RemoteQueryExecutorReadContext`.
#
# `clearAsyncEvent` used to store `is_in_progress = false` only after `timer.reset`. When
# `timer.reset` threw -- in production `TimerDescriptor::drain` built a temporary `Epoll` and
# `epoll_create1` failed with `EMFILE` -- the flag stayed set while the timer had already been
# disarmed and `connection_fd` had already been unregistered. The cancellation that followed then
# blocked forever in `cancelBefore` waiting on an epoll set that could never become ready. The
# stuck thread holds the pipeline's processor mutex, so `KILL QUERY` could not recover it either;
# only restarting the server could.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY_ID="rqe_clear_async_event_${CLICKHOUSE_DATABASE}_$$"

# shellcheck disable=SC2064
trap "${CLICKHOUSE_CLIENT} -q 'SYSTEM DISABLE FAILPOINT timer_descriptor_drain_fail' > /dev/null 2>&1" EXIT

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT timer_descriptor_drain_fail"

# A distributed read that has to wait on the socket goes through `clearAsyncEvent`, where
# `timer.reset` -> `drain` now throws. The query must fail promptly rather than wedge the
# read context.
#
# use_hedged_requests=0 picks the `MultiplexedConnections` + `RemoteQueryExecutorReadContext` path
# rather than `HedgedConnections` / `PacketReceiver`.
timeout 60 ${CLICKHOUSE_CLIENT} \
    --query_id "$QUERY_ID" \
    --use_hedged_requests 0 \
    --async_socket_for_remote 1 \
    --function_sleep_max_microseconds_per_block 10000000 \
    -q "SELECT sleepEachRow(0.2) FROM remote('127.0.0.2', numbers(10)) FORMAT Null" > /dev/null 2>&1
rc=$?

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT timer_descriptor_drain_fail"

# The point of the test: the query terminates. 124 is what `timeout` returns when it kills.
if [ "$rc" -eq 124 ]; then
    echo "FAIL: query did not terminate"
else
    echo "query terminated"
fi

# The wedged thread used to sit in `ExecutingGraph::cancel` holding the processor mutex, so the
# query stayed in `system.processes` forever and could not be killed.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'"

# The query must have actually hit the failpoint, otherwise this test silently proves nothing.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT exception_code = 710
    FROM system.query_log
    WHERE query_id = '$QUERY_ID' AND current_database = currentDatabase() AND type != 'QueryStart'
    ORDER BY event_time_microseconds DESC
    LIMIT 1"

# And the server still serves distributed queries afterwards.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM remote('127.0.0.2', numbers(10))"
