#!/usr/bin/env bash
# Quotas over the time-based profile events (`QueryTimeMicroseconds` and friends) must charge failed
# queries too: these events are incremented when the query's status is logged, so the quota has to
# be charged after that on the exception path, as it is on the success path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U1="user1_05354_${CLICKHOUSE_DATABASE}"
U2="user2_05354_${CLICKHOUSE_DATABASE}"
Q1="quota1_05354_${CLICKHOUSE_DATABASE}"
Q2="quota2_05354_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q1}, ${Q2}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U1}, ${U2}"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U1}, ${U2}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON system.* TO ${U1}, ${U2}"

echo "-- a failure during execution charges the time it took"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q1} FOR INTERVAL 100 year MAX QueryTimeMicroseconds = 1, SelectQueryTimeMicroseconds = 1 TO ${U1}"
${CLICKHOUSE_CLIENT} --user "${U1}" -q "SELECT sleepEachRow(0.01), throwIf(number = 5) FROM numbers(10) SETTINGS max_block_size = 1" 2>&1 | grep -o -m1 "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['QueryTimeMicroseconds'] >= 50000, profile_events['SelectQueryTimeMicroseconds'] >= 50000 FROM system.quotas_usage WHERE quota_name = '${Q1}'"
${CLICKHOUSE_CLIENT} --user "${U1}" -q "SELECT count() FROM numbers(1)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"

echo "-- a failure after the query is registered but before it starts charges the time as well"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q2} FOR INTERVAL 100 year MAX QueryTimeMicroseconds = 1 TO ${U2}"
${CLICKHOUSE_CLIENT} --user "${U2}" -q "SELECT count() FROM no_such_table_05354" 2>&1 | grep -o -m1 "UNKNOWN_TABLE"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['QueryTimeMicroseconds'] > 0 FROM system.quotas_usage WHERE quota_name = '${Q2}'"
${CLICKHOUSE_CLIENT} --user "${U2}" -q "SELECT count() FROM numbers(1)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"

cleanup
