#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# A client that goes away while the server is still streaming the result makes the next write to
# the socket fail, which cancels the output buffer. The handler must then drop the connection
# instead of trying to deliver `ErrorResponse` into the canceled buffer (that used to be a logical
# error, `Cannot write to canceled buffer`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PG_USER="postgresql_user_05099_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
"

# Every row is sent to the client as soon as it is produced, so the server keeps writing to the
# socket long after the client is gone.
MARKER="client_disconnect_05099_${CLICKHOUSE_DATABASE}"
LONG_QUERY="SELECT '${MARKER}', sleepEachRow(0.1) FROM numbers(3000)
    SETTINGS max_block_size = 1, max_threads = 1, max_execution_time = 0"

psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
    --no-align --quiet -c "${LONG_QUERY}" > /dev/null 2>&1 &
PSQL_PID=$!

function count_running()
{
    ${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.processes
        WHERE query LIKE '%${MARKER}%' AND query NOT LIKE '%system.processes%'"
}

for _ in {1..300}
do
    [[ "$(count_running)" == "1" ]] && break
    sleep 0.1
done

echo "--- the statement is running"
count_running

# Kill the client without letting it send `Terminate` or `CancelRequest`: the server only learns
# about it when a write to the socket fails.
kill -KILL "${PSQL_PID}"
wait "${PSQL_PID}" 2>/dev/null

for _ in {1..300}
do
    [[ "$(count_running)" == "0" ]] && break
    sleep 0.1
done

echo "--- after the client is gone, the statement is gone too"
count_running

echo "--- and the server did not try to write into the canceled socket buffer"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.text_log
    WHERE logger_name = 'PostgreSQLHandler' AND message LIKE '%Cannot write to canceled buffer%'"

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER}"
