#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `queries` counter is accounted by itself (not together with other counters). Its usage at the beginning
# of a new quota interval must not be added to the stale value of the ended interval: otherwise the first overflow
# resets the counters and drops the queries already executed in the new interval, so the limit is not enforced.
# Quotas, users and roles are server-global, so the names are made unique.

USER="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
QUOTA="q_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA} FOR INTERVAL 5 SECOND MAX queries = 4 TO ${USER}"

# The usage of a quota appears in `system.quotas_usage` after the first query of its user.
${CLICKHOUSE_CLIENT} --user ${USER} -q "SELECT 1 FROM numbers(1) FORMAT Null"

# Wait for the interval to end without querying the quota, because a query of `system.quotas_usage`
# after the end would start the new interval by itself.
function wait_until()
{
    while [ "$(date +%s)" -lt "$1" ]; do
        sleep 0.1
    done
}

# Start at the beginning of an interval, so that the first query surely belongs to it.
wait_until "$(${CLICKHOUSE_CLIENT} -q "SELECT toUnixTimestamp(end_time) FROM system.quotas_usage WHERE quota_name = '${QUOTA}'")"

# All the queries run in one session: a successful login starts the new interval by itself, which would hide the problem.
# The first three queries are accounted in the first interval (3 of 4 queries used). The sleeps read `system.one`,
# and queries reading only system tables are not accounted by quotas, so they cross the end of the interval
# without starting the new one. The last two queries are the first two of the new interval.
${CLICKHOUSE_CLIENT} --user ${USER} -q "
    SELECT 1 FROM numbers(1) FORMAT Null;
    SELECT 1 FROM numbers(1) FORMAT Null;
    SELECT 1 FROM numbers(1) FORMAT Null;
    SELECT sleep(3) FORMAT Null;
    SELECT sleep(2.5) FORMAT Null;
    SELECT 1 FROM numbers(1) FORMAT Null;
    SELECT 1 FROM numbers(1) FORMAT Null;
"

${CLICKHOUSE_CLIENT} -q "SELECT queries FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"

${CLICKHOUSE_CLIENT} -q "DROP USER ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${QUOTA}"
