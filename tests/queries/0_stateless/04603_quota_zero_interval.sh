#!/usr/bin/env bash
# A zero-length quota interval used to make the server divide by the interval
# duration when the quota was consumed (SIGFPE in EnabledQuota::getEndOfInterval).
# CREATE QUOTA must reject a non-positive interval instead.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A quota is a server-global entity, so scope the name to this test's database to
# stay safe when run in parallel with itself. Do NOT assign the quota to the
# shared `default` user: the interval check happens at CREATE time regardless of
# assignment, and a query quota on `default` would be consumed by every
# concurrent test's queries, exhaust the limit, and make the server reject them
# with QUOTA_EXCEEDED under stress/parallel runs.
Q="quota_zero_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"

${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 0 SECOND MAX queries = 1000" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
# Fractional interval that rounds down to zero seconds hits the same path.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 0.4 SECOND MAX queries = 1000" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# A positive interval still works and the server keeps running.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 1 HOUR MAX queries = 1000"
${CLICKHOUSE_CLIENT} -q "SELECT 'ok'"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
