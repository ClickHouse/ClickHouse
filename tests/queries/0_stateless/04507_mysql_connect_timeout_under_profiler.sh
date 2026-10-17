#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag justification:
#   no-fasttest: depends on libmysql (MySQL database engine), not built in fast test.
#   no-parallel: attaches a MySQL database pointing at an unreachable host; it is visible
#     in system.tables, so a concurrent unfiltered scan would also try to connect to it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `MYSQL_OPT_CONNECT_TIMEOUT` must bound the TCP connect even when the thread receives
# periodic signals: the sampling query profiler interrupts poll with EINTR, and the
# connector used to restart the poll with the full timeout, resetting the deadline every
# time. With a blackholed host (192.0.2.1, RFC 5737 TEST-NET-1: SYN packets are dropped,
# not refused) the connect then lasted until the kernel exhausted SYN retransmissions
# (~130 s) instead of connect_timeout. The profiler period is set well below
# connect_timeout so the poll is guaranteed to be interrupted several times.

MYSQL_DB="${CLICKHOUSE_DATABASE}_mysql"
ATTACH_QUERY_ID="${CLICKHOUSE_DATABASE}_mysql_timeout_attach_${RANDOM}${RANDOM}"

CLICKHOUSE_CLIENT_QUIET=$(echo "${CLICKHOUSE_CLIENT}" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=fatal/g")

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${MYSQL_DB}"

${CLICKHOUSE_CLIENT_QUIET} --query_id "${ATTACH_QUERY_ID}" \
    --query_profiler_real_time_period_ns 100000000 \
    -q "ATTACH DATABASE ${MYSQL_DB} ENGINE = MySQL('192.0.2.1:3306', 'fake_db', 'user', 'password') SETTINGS connect_timeout = 1, connection_max_tries = 1"

# The bound is 10x the configured connect_timeout (1 s): the fixed ATTACH takes ~1-2 s
# (one bounded connection attempt plus attach overhead), the broken one ~135 s, and even
# a partial regression (e.g. capping the retries instead of decrementing the budget)
# would exceed 10 s while slow CI machines stay well below it.
${CLICKHOUSE_CLIENT} -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT 'attach_bounded_by_timeout', query_duration_ms < 10000 FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${ATTACH_QUERY_ID}' AND type = 'QueryFinish';
"

${CLICKHOUSE_CLIENT_QUIET} -q "DROP DATABASE ${MYSQL_DB}"
