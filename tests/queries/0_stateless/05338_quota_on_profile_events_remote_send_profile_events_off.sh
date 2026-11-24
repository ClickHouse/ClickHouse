#!/usr/bin/env bash
# The remote servers of a distributed query report their profile events to the initiator, which
# charges them to quotas over profile events, even when the user turns `send_profile_events` off:
# otherwise the setting would bypass the limits over events that happen only on the remote servers.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05338_${CLICKHOUSE_DATABASE}"
Q="quota_05338_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_05338"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U} SETTINGS send_profile_events = 0"

# 127.0.0.2 is not a local address of the server, so the query goes over the network and the
# table is read by the remote server only: the initiator selects no parts itself.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05338 (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_05338 SELECT number FROM numbers(10)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_05338 FINAL"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE, REMOTE ON *.* TO ${U}"
# With `serialize_query_plan` the initiator analyzes the remote table itself, under this user.
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05338 TO ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX SelectedParts = 1 TO ${U}"

# Off both in the user's profile and in the query itself.
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT sum(a) FROM remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.t_05338) SETTINGS send_profile_events = 0"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['SelectedParts'] FROM system.quotas_usage WHERE quota_name = '${Q}'"
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT sum(a) FROM remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.t_05338) SETTINGS send_profile_events = 0"
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT sum(a) FROM remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.t_05338) SETTINGS send_profile_events = 0" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['SelectedParts'] FROM system.quotas_usage WHERE quota_name = '${Q}'"

cleanup
