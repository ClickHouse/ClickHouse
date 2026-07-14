#!/usr/bin/env bash
# A zero-length quota interval used to make the server divide by the interval
# duration when the quota was consumed (SIGFPE in EnabledQuota::getEndOfInterval).
# CREATE QUOTA must reject a non-positive interval instead.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Quota and user are server-global entities, so scope their names to this test's
# database to stay safe when run in parallel with itself. Assign to a dedicated
# user rather than default so the positive-path quota never touches other tests.
Q="quota_zero_${CLICKHOUSE_DATABASE}"
U="user_zero_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"

${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 0 SECOND MAX queries = 1000 TO ${U}" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
# Fractional interval that rounds down to zero seconds hits the same path.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 0.4 SECOND MAX queries = 1000 TO ${U}" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# A positive interval still works: run the query AS ${U} so it consumes the quota and
# exercises the previously-crashing getEndOfInterval path, then confirm usage was accounted.
# The query reads a table (numbers(1)) rather than a bare constant so it is charged to the quota.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 1 HOUR MAX queries = 1000 TO ${U}"
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT 'ok' FROM numbers(1)"
${CLICKHOUSE_CLIENT} -q "SELECT queries >= 1 FROM system.quotas_usage WHERE quota_name = '${Q}'"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
