#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# A PostgreSQL client cancels a running statement by opening a second connection and sending a
# `CancelRequest` on it, carrying the (process id, secret key) pair the server handed out in
# `BackendKeyData`. That connection never authenticates - the secret key is the credential - so the
# server must not need an authenticated session to honour it, and the key must identify the query that
# is actually running. `psql` sends exactly this on `SIGINT`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run: the flaky check runs this test many times concurrently,
# and a global name would collide with `ACCESS_ENTITY_ALREADY_EXISTS`.
PG_USER="postgresql_user_04669_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
"

# A marker in the query text, to find this exact query in `system.processes`. A single stream of
# single-row blocks makes the statement run for 300 seconds, and nothing but the cancel request may end
# it earlier.
MARKER="cancel_request_04669_${CLICKHOUSE_DATABASE}"
LONG_QUERY="SELECT '${MARKER}', sleepEachRow(0.1) FROM numbers(3000)
    SETTINGS max_block_size = 1, max_threads = 1, max_execution_time = 0"

PSQL_OUTPUT="${CLICKHOUSE_TMP}/04669_psql_output.txt"

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
# `BackendKeyData` gives the client `postgres:<connection id>:<secret key>`, so a query id of that shape
# is what the cancel request resolves to.
${CLICKHOUSE_CLIENT} -q "
    SELECT match(query_id, '^postgres:\\d+:\\d+\$') FROM system.processes
    WHERE query LIKE '%${MARKER}%' AND query NOT LIKE '%system.processes%'"

# This is what makes psql send a `CancelRequest` on a connection of its own.
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
grep -c -F 'Query was cancelled' "${PSQL_OUTPUT}"

rm -f "${PSQL_OUTPUT}"

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER}"
