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
${CLICKHOUSE_CLIENT} -q "GRANT SHOW QUOTAS ON *.* TO ${USER}"
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

# The scenario depends on the wall clock, and a slow run (e.g. under a sanitizer on a loaded machine) can miss
# the intervals it targets. Each attempt reports the end of the interval for its checkpoints, and an attempt
# whose checkpoints are not in the intended intervals is repeated.
for _ in {1..10}; do
    # Start at the beginning of an interval, so that the first query surely belongs to it.
    END=$(${CLICKHOUSE_CLIENT} -q "SELECT toUnixTimestamp(end_time) FROM system.quotas_usage WHERE quota_name = '${QUOTA}'")
    wait_until "${END}"

    # All the queries run in one session: a successful login starts the new interval by itself, which would hide the problem.
    # The first three queries are accounted in the first interval (3 of 4 queries used). The sleeps read `system.one`,
    # and queries reading only tables of the `system` database (including `system.quota_usage`) are not accounted by quotas, so they cross
    # the end of the interval without starting the new one. The last two queries are the first two of the new interval.
    # The usage is read in the same session, because a new client may start too late on a slow machine.
    # A slow attempt may also exceed the quota by accounting all its queries in one interval, so the errors are
    # ignored, and such an attempt is repeated as it does not report both checkpoints.
    RESULT=$(${CLICKHOUSE_CLIENT} --user ${USER} 2>/dev/null -q "
        SELECT 1 FROM numbers(1) FORMAT Null;
        SELECT 1 FROM numbers(1) FORMAT Null;
        SELECT 1 FROM numbers(1) FORMAT Null;
        SELECT toUnixTimestamp(end_time) FROM system.quota_usage WHERE quota_name = '${QUOTA}';
        SELECT sleep(3) FORMAT Null;
        SELECT sleep(2.5) FORMAT Null;
        SELECT 1 FROM numbers(1) FORMAT Null;
        SELECT 1 FROM numbers(1) FORMAT Null;
        SELECT toUnixTimestamp(end_time), queries FROM system.quota_usage WHERE quota_name = '${QUOTA}' FORMAT TSV;
    ")

    read -r -d '' FIRST_END SECOND_END QUERIES <<< "${RESULT}"
    if [ "${FIRST_END}" = "$((END + 5))" ] && [ "${SECOND_END}" = "$((END + 10))" ]; then
        echo "${QUERIES}"
        break
    fi
done

${CLICKHOUSE_CLIENT} -q "DROP USER ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${QUOTA}"
