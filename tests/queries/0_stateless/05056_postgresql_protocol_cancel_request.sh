#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# `CancelRequest` authenticates with the process ID and secret from `BackendKeyData`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Avoid user-name collisions in concurrent flaky checks.
PG_USER="postgresql_user_05056_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
"

# Keep a marked query visible in `system.processes` until cancellation.
MARKER="cancel_request_05056_${CLICKHOUSE_DATABASE}"
LONG_QUERY="SELECT '${MARKER}', sleepEachRow(0.1) FROM numbers(3000)
    SETTINGS max_block_size = 1, max_threads = 1, max_execution_time = 0"

PSQL_OUTPUT="${CLICKHOUSE_TMP}/05056_psql_output.txt"

psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
    --no-align --quiet -c "${LONG_QUERY}" > "${PSQL_OUTPUT}" 2>&1 &
PSQL_PID=$!

function count_running()
{
    ${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.processes
        WHERE query LIKE '%${MARKER}%' AND query NOT LIKE '%system.processes%'"
}

# Wait for the statement to start running on the server.
for _ in {1..300}
do
    [[ "$(count_running)" == "1" ]] && break
    sleep 0.1
done

echo "--- the statement is running"
count_running

echo "--- under a query id the client can cancel"
# The connection ID from `BackendKeyData` maps to `postgres:<connection id>:<token>`. The token is
# not the secret from that message: the secret authenticates `CancelRequest` and is kept server side.
${CLICKHOUSE_CLIENT} -q "
    SELECT match(query_id, '^postgres:\\d+:\\d+\$') FROM system.processes
    WHERE query LIKE '%${MARKER}%' AND query NOT LIKE '%system.processes%'"

# Make `psql` send `CancelRequest`.
kill -INT "${PSQL_PID}"
wait "${PSQL_PID}" 2>/dev/null

for _ in {1..300}
do
    [[ "$(count_running)" == "0" ]] && break
    sleep 0.1
done

echo "--- after the cancel request, the statement is gone"
count_running

echo "--- and the client was told why"
rg -c -F 'Query was cancelled' "${PSQL_OUTPUT}"

rm -f "${PSQL_OUTPUT}"

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER}"
