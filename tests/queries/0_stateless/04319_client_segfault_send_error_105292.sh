#!/usr/bin/env bash
# Tags: no-random-settings, no-parallel, no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/105292.
# Without the fix in `ClientBase::processOrdinaryQuery`, this would SIGSEGV at
# Address 0x108 in `ReadBufferFromPocoSocketChunked::poll`. The proxy helper
# forces a TCP RST on the client mid-write, which is the scenario where
# `Connection::sendQuery`'s SCOPE_EXIT resets `in` to nullptr.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PROXY="${CUR_DIR}/helpers/04319_client_segfault_proxy.py"

TMP_DIR=${CLICKHOUSE_TMP:-/tmp}/04319_client_segfault_105292
mkdir -p "${TMP_DIR}"
PROXY_LOG="${TMP_DIR}/proxy.log"
PROXY_PORT_FILE="${TMP_DIR}/proxy.port"
INSERT_SQL="${TMP_DIR}/insert.sql"
CLIENT_LOG="${TMP_DIR}/client.log"

# 200k rows of 5 UUIDs each is ~40 MB once formatted as Values, more than any
# realistic loopback send buffer so the client keeps writing past the cutoff
# and observes the RST.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_105292"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_105292
    (a UUID, b UUID, c UUID, d UUID, e UUID)
    ENGINE = MergeTree() ORDER BY ()"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO test_105292
    SELECT generateUUIDv4(), generateUUIDv4(), generateUUIDv4(), generateUUIDv4(), generateUUIDv4()
    FROM numbers(200000)"

echo "INSERT INTO test_105292 VALUES" > "${INSERT_SQL}"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_105292 FORMAT Values" >> "${INSERT_SQL}"

rm -f "${PROXY_PORT_FILE}"

# Single-shot proxy: serves one connection, then exits. No external `kill` is
# needed, so bash job control never emits a "Killed" notification that would
# otherwise be reported as test stderr noise by the runner.
python3 "${PROXY}" "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_TCP}" 100000 "${PROXY_PORT_FILE}" \
    >"${PROXY_LOG}" 2>&1 &
PROXY_PID=$!

for _ in $(seq 1 50); do
    [ -s "${PROXY_PORT_FILE}" ] && break
    sleep 0.1
done
PROXY_PORT=$(cat "${PROXY_PORT_FILE}" 2>/dev/null || echo "")

if [ -z "${PROXY_PORT}" ]; then
    echo "FAIL: proxy did not start"
    cat "${PROXY_LOG}"
    wait "${PROXY_PID}" 2>/dev/null
    exit 1
fi

# Invoke the client binary directly (NOT `${CLICKHOUSE_CLIENT}`) because
# `${CLICKHOUSE_CLIENT}` already carries the runner-injected `--host=` and
# `--port=` of the real server. Boost program_options keeps the FIRST value
# of a repeated CLI option for `--port`, so an appended `--port=${PROXY_PORT}`
# would silently be ignored and the client would connect straight to the
# server, bypassing the proxy and the regression path entirely.
# `--send_logs_level=none` keeps server log packets out of the client's
# fatal-log file (the abrupt close races with chunked I/O on the server side
# and would otherwise pollute it with an unrelated stack trace).
${CLICKHOUSE_CLIENT_BINARY} --send_logs_level=none \
    --database="${CLICKHOUSE_DATABASE}" \
    --host=127.0.0.1 --port="${PROXY_PORT}" \
    < "${INSERT_SQL}" >"${CLIENT_LOG}" 2>&1
CLIENT_EXIT=$?

# Proxy exits on its own after RSTing the single connection. Capture its exit
# status so we can assert the regression path (cutoff exceeded + RST) was
# actually exercised, not bypassed by an early target-connect / EOF failure.
wait "${PROXY_PID}"
PROXY_EXIT=$?

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_105292"

# 139 = SIGSEGV. Reject the original bug shape explicitly.
if [ "${CLIENT_EXIT}" = "139" ]; then
    echo "BAD: client segfaulted (exit ${CLIENT_EXIT})"
    head -50 "${CLIENT_LOG}"
    exit 1
fi
if grep -q "Address: 0x108\|Segmentation fault" "${CLIENT_LOG}"; then
    echo "BAD: crash signature in client output (exit ${CLIENT_EXIT})"
    head -50 "${CLIENT_LOG}"
    exit 1
fi

# Proxy exits 0 only after forwarding more than `cutoff` bytes and forcing the
# RST. Any other exit means the client never reached the mid-write cutoff path
# (target-connect failure, accept timeout, EOF or send error before cutoff),
# in which case a passing client run would not actually exercise the regression.
if [ "${PROXY_EXIT}" != "0" ]; then
    echo "BAD: proxy did not exercise the mid-write cutoff path (PROXY_EXIT=${PROXY_EXIT})"
    echo "--- proxy log ---"
    cat "${PROXY_LOG}"
    echo "--- client log ---"
    head -50 "${CLIENT_LOG}"
    exit 1
fi
if ! grep -q "CUTOFF_EXCEEDED" "${PROXY_LOG}"; then
    echo "BAD: proxy did not log CUTOFF_EXCEEDED marker (PROXY_EXIT=${PROXY_EXIT})"
    echo "--- proxy log ---"
    cat "${PROXY_LOG}"
    exit 1
fi

# Require evidence the send-error path actually fired. A clean INSERT (cutoff
# missed, buffer absorbed it) would silently stop catching the bug.
if [ "${CLIENT_EXIT}" = "0" ]; then
    echo "BAD: proxy did not trigger the send-error path (CLIENT_EXIT=0)"
    head -50 "${CLIENT_LOG}"
    exit 1
fi
if ! grep -qE "NETWORK_ERROR|Broken pipe|Connection reset by peer" "${CLIENT_LOG}"; then
    echo "BAD: no network-error marker in client output (CLIENT_EXIT=${CLIENT_EXIT})"
    head -50 "${CLIENT_LOG}"
    exit 1
fi

echo OK
