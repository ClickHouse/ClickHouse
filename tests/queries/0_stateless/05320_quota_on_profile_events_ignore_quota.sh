#!/usr/bin/env bash
# Queries exempt from quotas (here: reading only `system.one` and `SHOW CREATE QUOTA`) are charged
# against the quotas over profile events and the `errors` quota neither when they succeed nor when
# they fail; other failed queries are.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05320_${CLICKHOUSE_DATABASE}"
Q="quota_05320_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX errors = 100, FailedQuery = 100, Query = 100 TO ${U}"

echo "-- exempt queries"
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT * FROM system.one"
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT throwIf(dummy = 0) FROM system.one" 2>&1 | grep -o -m1 "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"
# Fails before the query starts, after the interpreter has exempted it from quotas.
${CLICKHOUSE_CLIENT} --user "${U}" -q "SHOW CREATE QUOTA quota_05320_missing" >/dev/null 2>&1 || echo "failed"
${CLICKHOUSE_CLIENT} -q "SELECT errors, profile_events['FailedQuery'], profile_events['Query'] FROM system.quotas_usage WHERE quota_name = '${Q}'"

echo "-- not exempt"
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT throwIf(number = 0) FROM numbers(1)" 2>&1 | grep -o -m1 "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"
${CLICKHOUSE_CLIENT} -q "SELECT errors, profile_events['FailedQuery'], profile_events['Query'] FROM system.quotas_usage WHERE quota_name = '${Q}'"

cleanup
