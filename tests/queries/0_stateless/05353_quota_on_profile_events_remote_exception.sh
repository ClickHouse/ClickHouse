#!/usr/bin/env bash
# A remote server that fails a query sends the tail of its profile events before the exception,
# so the initiator charges them to the quotas over profile events of the initiating user.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05353_${CLICKHOUSE_DATABASE}"
Q="quota_05353_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_05353"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05353 (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_05353 SELECT number FROM numbers(10)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_05353 FINAL"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE, REMOTE ON *.* TO ${U}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05353 TO ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX SelectedParts = 1000 TO ${U}"

# 127.0.0.2 is not a local address of the server, so the table is read by the remote server only,
# and the query fails there while reading it.
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT throwIf(a = 9) FROM remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.t_05353)" 2>&1 | grep -o -m1 "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['SelectedParts'] FROM system.quotas_usage WHERE quota_name = '${Q}'"

cleanup
