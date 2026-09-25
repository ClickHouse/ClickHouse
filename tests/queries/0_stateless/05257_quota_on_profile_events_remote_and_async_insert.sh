#!/usr/bin/env bash
# Quotas over profile events also account the work done outside the local query thread group:
# on the remote servers executing parts of a distributed query, and in the deferred flush of
# asynchronous inserts, which runs as an internal query on behalf of the submitting user.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U1="user1_05257_${CLICKHOUSE_DATABASE}"
U2="user2_05257_${CLICKHOUSE_DATABASE}"
Q1="quota1_05257_${CLICKHOUSE_DATABASE}"
Q2="quota2_05257_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q1}, ${Q2}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U1}, ${U2}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_remote_05257"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_async_05257"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U1}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${U2}"

# 127.0.0.2 is not a local address of the server, so the query goes over the network and the
# table is read by the remote server only: the initiator selects no parts itself.
# `SelectedRows` is also incremented on the initiator from the progress the remote server
# reports, and must not be counted twice.
echo "-- remote: SelectedParts of the remote server are charged to the initiator"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_remote_05257 (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_remote_05257 SELECT number FROM numbers(10)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_remote_05257 FINAL"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE, REMOTE ON *.* TO ${U1}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q1} FOR INTERVAL 100 year MAX SelectedParts = 1, SelectedRows = 1000 TO ${U1}"
${CLICKHOUSE_CLIENT} --user "${U1}" -q "SELECT sum(a) FROM remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.t_remote_05257)"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['SelectedParts'], profile_events['SelectedRows'] FROM system.quotas_usage WHERE quota_name = '${Q1}'"
${CLICKHOUSE_CLIENT} --user "${U1}" -q "SELECT sum(a) FROM remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.t_remote_05257)"
${CLICKHOUSE_CLIENT} --user "${U1}" -q "SELECT sum(a) FROM remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.t_remote_05257)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['SelectedParts'], profile_events['SelectedRows'] FROM system.quotas_usage WHERE quota_name = '${Q1}'"

# With `wait_for_async_insert = 1` the insert returns after the flush, which is accounted before
# the waiting clients are notified.
echo "-- async insert: the deferred flush is charged to the submitting user"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_async_05257 (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_async_05257 TO ${U2}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q2} FOR INTERVAL 100 year MAX InsertedRows = 1000, AsyncInsertFlush = 1000 TO ${U2}"
${CLICKHOUSE_CLIENT} --user "${U2}" --async_insert 1 --wait_for_async_insert 1 -q "INSERT INTO t_async_05257 VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_async_05257"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['InsertedRows'], profile_events['AsyncInsertFlush'] FROM system.quotas_usage WHERE quota_name = '${Q2}'"

cleanup
