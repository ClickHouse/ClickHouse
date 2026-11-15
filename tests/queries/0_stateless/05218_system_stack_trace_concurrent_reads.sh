#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Both reads of this table in one plan are built one after the other on the same thread, so an exclusion
# between readers that spans pipeline construction leaves the second one waiting on a mutex its own thread
# already holds. The oracle is the outer timeout: both rows must arrive.
# One thread per block, five blocks: the cost does not depend on how many threads the server runs, and
# each read passes through the exclusion five times instead of once.
if timeout 60 ${CLICKHOUSE_CLIENT} --max_execution_time 50 --query "
        SELECT count(trace) > 0 FROM (SELECT trace FROM system.stack_trace LIMIT 5 SETTINGS max_block_size = 1)
        UNION ALL
        SELECT count(trace) > 0 FROM (SELECT trace FROM system.stack_trace LIMIT 5 SETTINGS max_block_size = 1)
    " 2>&1 | tr '\n' ' ' | grep -q -E '^1 1 *$'
then
    echo 'two reads in one query finished'
else
    echo 'two reads in one query did not finish'
fi

# The exclusion covers one collected block, not a whole query, so a reader whose rows go to a slow
# consumer must not keep other readers out for as long as its own query runs.
HOLDER_ID="stack_trace_holder_${CLICKHOUSE_DATABASE}"
HOLDER_ERR="${CLICKHOUSE_TMP}/05218_holder_${CLICKHOUSE_TEST_UNIQUE_NAME}.stderr"
${CLICKHOUSE_CLIENT} --query_id "${HOLDER_ID}" --query "
    SELECT sleepEachRow(2), length(trace) FROM system.stack_trace LIMIT 30
    SETTINGS max_block_size = 1, max_execution_time = 120 FORMAT Null" 2>"${HOLDER_ERR}" &
HOLDER_PID=$!

for _ in {1..100}
do
    [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '${HOLDER_ID}'")" = "1" ] && break
    sleep 0.2
done
sleep 1

# Without a live holder the probe below measures an uncontended mutex and would pass while testing nothing.
if [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '${HOLDER_ID}'")" != "1" ]
then
    echo 'holder is not running'
    cat "${HOLDER_ERR}"
fi

if timeout 30 ${CLICKHOUSE_CLIENT} --max_execution_time 25 -q "
        SELECT count(trace) > 0 FROM (SELECT trace FROM system.stack_trace LIMIT 5 SETTINGS max_block_size = 1)" | grep -q -F '1'
then
    probe_ok=1
else
    probe_ok=0
fi

# A holder that exited during the probe leaves an uncontended mutex, so the probe would pass while
# testing nothing.
if [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '${HOLDER_ID}'")" != "1" ]
then
    echo 'holder exited during the probe'
    cat "${HOLDER_ERR}"
elif [ "$probe_ok" = "1" ]
then
    echo 'a concurrent reader is not blocked by a slow consumer'
else
    echo 'a concurrent reader is blocked by a slow consumer'
fi

# The holder is not needed any more. It reports QUERY_WAS_CANCELLED, which is not a test failure.
${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '${HOLDER_ID}' ASYNC FORMAT Null" 2>/dev/null
kill "${HOLDER_PID}" 2>/dev/null
wait "${HOLDER_PID}" 2>/dev/null || true
rm -f "${HOLDER_ERR}"
