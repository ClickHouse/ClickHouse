#!/usr/bin/env bash
# Tags: no-parallel
# Reason: enables a server-wide REGULAR failpoint that affects all threads
# Regression test for OSIOWaitMicroseconds being polluted with thread-lifetime
# blkio accumulation when TasksStatsCounters::reset throws.
#
# When reset() fails, the fix nulls out taskstats so no counter update runs,
# keeping OSIOWaitMicroseconds at 0 instead of inheriting the thread's kernel
# blkio_delay_total (which can be on the order of days for long-lived workers).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Suppress Warning-level server logs from being forwarded to client stderr;
# this test deliberately triggers a Warning by injecting a reset() failure.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --send_logs_level=fatal"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT taskstats_counters_reset_throw"
trap '${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT taskstats_counters_reset_throw" || true' EXIT

QUERY_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} --query_id="${QUERY_ID}" --query "SELECT sum(number) FROM numbers(1000) FORMAT Null"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# OSIOWaitMicroseconds must be 0: taskstats was nulled out on reset failure,
# so updateCounters was never called and the counter was never incremented.
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['OSIOWaitMicroseconds'] = 0
    FROM system.query_log
    WHERE query_id = '${QUERY_ID}' AND type = 'QueryFinish'
        AND current_database = currentDatabase()
    LIMIT 1
"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT taskstats_counters_reset_throw"
