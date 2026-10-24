#!/usr/bin/env bash
# Quotas over profile events account the work of the remote servers for a distributed `INSERT`:
# the remote server sends its profile events after receiving all the data, and the initiator
# charges them to the initiating user. `MergeTreeDataWriterRows` is produced by the remote server
# only, while `InsertedRows` is counted by the initiator itself and must not be charged twice.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05385_${CLICKHOUSE_DATABASE}"
Q="quota_05385_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_05385"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05385 (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE, REMOTE ON *.* TO ${U}"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT, SELECT ON ${CLICKHOUSE_DATABASE}.t_05385 TO ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX MergeTreeDataWriterRows = 5, InsertedRows = 100 TO ${U}"

# 127.0.0.2 is not a local address of the server, so the data goes over the network and the
# rows are inserted by the remote server only. An `INSERT` into the `remote` table function is
# always sent synchronously. Asynchronous inserts would charge the deferred flush instead.
${CLICKHOUSE_CLIENT} --user "${U}" --async_insert 0 -q "INSERT INTO FUNCTION remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.t_05385) VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_05385"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['MergeTreeDataWriterRows'], profile_events['InsertedRows'] FROM system.quotas_usage WHERE quota_name = '${Q}'"
# The reports of the remote server do not depend on `send_profile_events` of the query.
${CLICKHOUSE_CLIENT} --user "${U}" --async_insert 0 --send_profile_events 0 -q "INSERT INTO FUNCTION remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.t_05385) SELECT number FROM numbers(3)"
${CLICKHOUSE_CLIENT} --user "${U}" --async_insert 0 -q "INSERT INTO FUNCTION remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.t_05385) VALUES (7)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_05385"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['MergeTreeDataWriterRows'], profile_events['InsertedRows'] FROM system.quotas_usage WHERE quota_name = '${Q}'"

cleanup
