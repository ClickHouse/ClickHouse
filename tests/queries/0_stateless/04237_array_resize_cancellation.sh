#!/usr/bin/env bash
# Tags: no-sanitizers, no-debug
# Reason: timing assertion is flaky on slow builds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `arrayResize` must check query cancellation to respect timeouts.
# The ARRAY JOIN list runs the resize once per element. Three copies make the natural
# completion time on the order of tens of seconds even on the fastest CI machines, so
# the 20s threshold below clearly distinguishes "fix in effect" from "running to
# completion." On the unpatched binary the same query takes well over a minute.

START_NS=$(date +%s%N)
${CLICKHOUSE_CLIENT} --max_threads 1 --max_execution_time 1 -q "
    SELECT sum(length(arrayResize(a, b))) FROM (
        SELECT a FROM generateRandom(
            'a Array(Nested(e1 Tuple(x Int256, y Float64, z Decimal(38, 10))))',
            1, 4, 4) LIMIT 1)
    ARRAY JOIN [-1000000000, -1000000000, -1000000000]::Array(Int32) AS b
    FORMAT Null
" 2>&1 | grep -q -E 'TIMEOUT_EXCEEDED|QUERY_WAS_CANCELLED' && echo "got expected timeout exception"
ELAPSED_MS=$(( ($(date +%s%N) - START_NS) / 1000000 ))

if (( ELAPSED_MS < 10000 ))
then
    echo "elapsed under 10s"
else
    echo "FAIL: elapsed ${ELAPSED_MS}ms"
fi
