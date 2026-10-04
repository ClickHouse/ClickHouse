#!/usr/bin/env bash
# Quotas over the `FailedQuery` family of profile events. The failure itself is a profile event
# that is incremented on the exception path, after the query's counters are finalized, so it
# must be accounted against the quota on both paths: a failure during execution and a failure
# before execution starts (e.g. a syntax error, when there is no process list entry yet).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U1="user1_05218_${CLICKHOUSE_DATABASE}"
U2="user2_05218_${CLICKHOUSE_DATABASE}"
U3="user3_05218_${CLICKHOUSE_DATABASE}"
Q1="quota1_05218_${CLICKHOUSE_DATABASE}"
Q2="quota2_05218_${CLICKHOUSE_DATABASE}"
Q3="quota3_05218_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q1}, ${Q2}, ${Q3}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U1}, ${U2}, ${U3}"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U1}, ${U2}, ${U3}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON system.* TO ${U1}, ${U2}, ${U3}"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE ON *.* TO ${U1}, ${U2}, ${U3}"

# Note: SELECT without FROM reads only `system.one` and such queries are exempt from quotas
# by design, so the test queries must read a real table.

echo "-- a failure during execution is charged; the next query is rejected"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q1} FOR INTERVAL 100 year MAX FailedQuery = 1 TO ${U1}"
${CLICKHOUSE_CLIENT} --user "${U1}" -q "SELECT count() FROM numbers(1)"
${CLICKHOUSE_CLIENT} --user "${U1}" -q "SELECT throwIf(number = 5) FROM numbers(10)" 2>&1 | grep -o -m1 "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['FailedQuery'], max_profile_events['FailedQuery'] FROM system.quotas_usage WHERE quota_name = '${Q1}'"
${CLICKHOUSE_CLIENT} --user "${U1}" -q "SELECT throwIf(number = 5) FROM numbers(10)" 2>&1 | grep -o -m1 "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['FailedQuery'], max_profile_events['FailedQuery'] FROM system.quotas_usage WHERE quota_name = '${Q1}'"
${CLICKHOUSE_CLIENT} --user "${U1}" -q "SELECT count() FROM numbers(1)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"

echo "-- a failure before the query starts (syntax error) is charged as well"
# The queries go over HTTP: the native client parses a query itself and never sends a malformed one.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q2} FOR INTERVAL 100 year MAX FailedQuery = 1 TO ${U2}"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${U2}" -d "SELECT count() FROM numbers(1)"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${U2}" -d "SELECT count() FROM numbers(1) WHERE" 2>&1 | grep -o -m1 "SYNTAX_ERROR"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${U2}" -d "SELECT count() FROM numbers(1) WHERE" 2>&1 | grep -o -m1 "SYNTAX_ERROR"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['FailedQuery'], max_profile_events['FailedQuery'] FROM system.quotas_usage WHERE quota_name = '${Q2}'"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${U2}" -d "SELECT count() FROM numbers(1)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"

echo "-- a failure after the query is registered but before it starts (unknown table) is charged as well"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q3} FOR INTERVAL 100 year MAX FailedSelectQuery = 1 TO ${U3}"
${CLICKHOUSE_CLIENT} --user "${U3}" -q "SELECT count() FROM numbers(1)"
${CLICKHOUSE_CLIENT} --user "${U3}" -q "SELECT count() FROM no_such_table_05218" 2>&1 | grep -o -m1 "UNKNOWN_TABLE"
${CLICKHOUSE_CLIENT} --user "${U3}" -q "SELECT count() FROM no_such_table_05218" 2>&1 | grep -o -m1 "UNKNOWN_TABLE"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['FailedSelectQuery'], max_profile_events['FailedSelectQuery'] FROM system.quotas_usage WHERE quota_name = '${Q3}'"
${CLICKHOUSE_CLIENT} --user "${U3}" -q "SELECT count() FROM numbers(1)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"

cleanup
