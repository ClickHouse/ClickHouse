#!/usr/bin/env bash
# Quotas defined over arbitrary profile events: enforcement, introspection, and errors.
# Profile events are accounted once per query, at its end, so a query that crosses the
# limit still finishes and the subsequent queries are rejected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
# Do not assign anything to shared users like `default`.
U1="user1_05054_${CLICKHOUSE_DATABASE}"
U2="user2_05054_${CLICKHOUSE_DATABASE}"
Q1="quota1_05054_${CLICKHOUSE_DATABASE}"
Q2="quota2_05054_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q1}, ${Q2}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U1}, ${U2}"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U1}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${U2}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON system.* TO ${U1}, ${U2}"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE ON *.* TO ${U1}, ${U2}"

echo "-- unknown profile event name is rejected"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q1} FOR INTERVAL 100 year MAX NoSuchProfileEvent = 1 TO ${U1}" 2>&1 | grep -o -m1 "SYNTAX_ERROR"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q1} FOR INTERVAL 100 year MAX quries = 1 TO ${U1}" 2>&1 | grep -o -m1 "SYNTAX_ERROR"

echo "-- SHOW CREATE QUOTA round trip"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q1} FOR INTERVAL 100 year MAX queries = 100, Query = 2, SelectedRows = 1000 TO ${U1}"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE QUOTA ${Q1}" | sed "s/${CLICKHOUSE_DATABASE}/db/g"

echo "-- system.quota_limits"
${CLICKHOUSE_CLIENT} -q "SELECT max_queries, max_profile_events FROM system.quota_limits WHERE quota_name = '${Q1}'"

# Note: SELECT without FROM reads only `system.one` and such queries are exempt from quotas
# by design (to let the user inspect the quota state when it is exhausted), so the test
# queries must read a real table.
echo "-- the third query crosses the limit and still finishes, the fourth is rejected"
${CLICKHOUSE_CLIENT} --user "${U1}" -q "SELECT count() FROM numbers(1)"
${CLICKHOUSE_CLIENT} --user "${U1}" -q "SELECT count() FROM numbers(1)"
${CLICKHOUSE_CLIENT} --user "${U1}" -q "SELECT count() FROM numbers(1)"
${CLICKHOUSE_CLIENT} --user "${U1}" -q "SELECT count() FROM numbers(1)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"

echo "-- current consumption is reported in system.quotas_usage"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['Query'], max_profile_events['Query'] FROM system.quotas_usage WHERE quota_name = '${Q1}'"

echo "-- raising the limit lets the queries pass again and keeps the counter"
${CLICKHOUSE_CLIENT} -q "ALTER QUOTA ${Q1} FOR INTERVAL 100 year MAX Query = 10"
${CLICKHOUSE_CLIENT} --user "${U1}" -q "SELECT count() FROM numbers(1)"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['Query'], max_profile_events['Query'] FROM system.quotas_usage WHERE quota_name = '${Q1}'"

echo "-- lowering the limit below the kept consumption blocks the queries"
${CLICKHOUSE_CLIENT} -q "ALTER QUOTA ${Q1} FOR INTERVAL 100 year MAX Query = 1"
${CLICKHOUSE_CLIENT} --user "${U1}" -q "SELECT count() FROM numbers(1)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"

echo "-- a resource-metering event: SelectedRows"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q2} FOR INTERVAL 100 year MAX SelectedRows = 20 TO ${U2}"
${CLICKHOUSE_CLIENT} --user "${U2}" -q "SELECT sum(number) FROM numbers(15)"
${CLICKHOUSE_CLIENT} --user "${U2}" -q "SELECT sum(number) FROM numbers(15)"
${CLICKHOUSE_CLIENT} --user "${U2}" -q "SELECT count() FROM numbers(1)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['SelectedRows'], max_profile_events['SelectedRows'] FROM system.quotas_usage WHERE quota_name = '${Q2}'"

cleanup
