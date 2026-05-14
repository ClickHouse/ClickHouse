#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for the deadlock observed when the speculative
# `additional_memory_tracking_per_thread` reservation throws inside a
# concurrently-running pipeline: if the throw happened before the query's
# thread group was attached, the job was skipped and the pipeline consumer
# blocked forever on the empty output queue.
#
# The fix routes the throw through each pipeline lambda's own try/catch after
# `ThreadGroupSwitcher`, so the failure propagates as a normal query error.
# Here we launch many concurrent SELECTs whose `max_memory_usage` is below the
# speculative reservation. Each must finish (with or without a memory-limit
# error) within `timeout` -- a hang would manifest as the timeout firing.

N=64
TIMEOUT=15

failures=0
for _ in $(seq 1 $N); do
    timeout "$TIMEOUT" $CLICKHOUSE_CLIENT --query "
        SELECT count() FROM numbers_mt(10000)
        SETTINGS max_threads = 4, max_memory_usage = 1
    " > /dev/null 2>&1 &
done

# Wait for every background client; record any that did not exit
# (i.e. the timeout fired -- which would indicate a hang).
hung=0
for pid in $(jobs -p); do
    if ! wait "$pid"; then
        rc=$?
        # 124 = exit code from `timeout` when the timeout actually fires.
        # Any other non-zero is fine (likely 241 / MEMORY_LIMIT_EXCEEDED).
        if [ "$rc" -eq 124 ]; then
            hung=$((hung + 1))
        fi
    fi
done

echo "hung=$hung"

# After the storm, the server must still answer trivial queries.
$CLICKHOUSE_CLIENT --query "SELECT 'survived'"
