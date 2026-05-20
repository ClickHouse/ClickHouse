#!/usr/bin/env bash
# Tags: no-random-settings, no-parallel, no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/105292
#
# Before the fix, `clickhouse-client < big_insert.sql` would crash with a SIGSEGV
# at `Address: 0x108` inside `ReadBufferFromPocoSocketChunked::poll` whenever
# `Connection::sendQuery` threw a `NetException` mid-write. The cause: `sendQuery`
# has a `SCOPE_EXIT` that calls `Connection::disconnect` if the query did not
# complete, which resets `in` to nullptr. The `catch (NetException)` block in
# `processOrdinaryQuery` then called `receiveResult`, which called
# `Connection::poll`, which dereferenced the now-null `in`.
#
# We reproduce the scenario by routing the client through a tiny Python TCP
# proxy that drops the connection after a small number of bytes. The
# `out->next()` inside `sendQuery` then raises a `NetException`, the
# `SCOPE_EXIT` disconnects, and the receive-drain path used to crash.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR=${CLICKHOUSE_TMP:-/tmp}/04259_client_segfault_105292
mkdir -p "${TMP_DIR}"

PROXY_SCRIPT="${TMP_DIR}/proxy.py"
PROXY_LOG="${TMP_DIR}/proxy.log"
PROXY_PORT_FILE="${TMP_DIR}/proxy.port"
INSERT_SQL="${TMP_DIR}/insert.sql"
CLIENT_LOG="${TMP_DIR}/client.log"

cat >"${PROXY_SCRIPT}" <<'PYEOF'
import os
import socket
import struct
import sys
import threading

target_host = sys.argv[1]
target_port = int(sys.argv[2])
cutoff = int(sys.argv[3])
port_file = sys.argv[4]

# Force an RST (TCP reset) on close so the client gets ECONNRESET on its
# next write immediately, instead of having data sit in a large local-loopback
# send buffer (Linux auto-tuned send buffers on loopback can be many MB).
def force_rst(sock):
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    except Exception:
        pass

def handle(client_sock):
    try:
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.connect((target_host, target_port))
    except Exception:
        try: client_sock.close()
        except Exception: pass
        return

    cutoff_event = threading.Event()

    # server -> client: relay until cutoff has been hit on the other direction
    def s2c():
        try:
            while not cutoff_event.is_set():
                data = server_sock.recv(8192)
                if not data:
                    break
                try:
                    client_sock.sendall(data)
                except Exception:
                    break
        except Exception:
            pass

    threading.Thread(target=s2c, daemon=True).start()

    # client -> server: relay with a byte cutoff, then RST both ends
    sent = 0
    try:
        while True:
            data = client_sock.recv(8192)
            if not data:
                break
            try:
                server_sock.sendall(data)
            except Exception:
                break
            sent += len(data)
            if sent > cutoff:
                break
    except Exception:
        pass
    finally:
        cutoff_event.set()
        force_rst(client_sock)
        force_rst(server_sock)
        try: client_sock.close()
        except Exception: pass
        try: server_sock.close()
        except Exception: pass

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", 0))
s.listen(8)
port = s.getsockname()[1]
with open(port_file + ".tmp", "w") as f:
    f.write(str(port))
os.rename(port_file + ".tmp", port_file)
while True:
    c, _ = s.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
PYEOF

# Build a sufficiently large INSERT VALUES payload. Loopback send buffers on
# Linux are auto-tuned and can reach several MB, so the payload must be large
# enough that the client keeps trying to write past the proxy's cutoff and
# observes the RST. 200k rows x 5 UUIDs ~= 40 MB easily overflows any
# realistic buffer.
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
python3 "${PROXY_SCRIPT}" "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_TCP}" 100000 "${PROXY_PORT_FILE}" \
    >"${PROXY_LOG}" 2>&1 &
PROXY_PID=$!

# Wait for the proxy to publish its listen port.
for _ in $(seq 1 50); do
    [ -s "${PROXY_PORT_FILE}" ] && break
    sleep 0.1
done
PROXY_PORT=$(cat "${PROXY_PORT_FILE}" 2>/dev/null || echo "")

if [ -z "${PROXY_PORT}" ]; then
    echo "FAIL: proxy did not start"
    kill -9 "${PROXY_PID}" 2>/dev/null
    cat "${PROXY_LOG}"
    exit 1
fi

# Route the client through the proxy. Expect a clean NetException, NOT a SIGSEGV.
# `--send_logs_level=none` keeps server-side log packets out of the client's fatal-log
# file (the connection close races with chunked I/O on the server and would otherwise
# pollute it with a stack trace that has nothing to do with this test).
# `--allow_repeated_settings` lets the override win over the runner-injected value.
${CLICKHOUSE_CLIENT} --allow_repeated_settings --send_logs_level=none \
    --host=127.0.0.1 --port="${PROXY_PORT}" \
    < "${INSERT_SQL}" >"${CLIENT_LOG}" 2>&1
CLIENT_EXIT=$?

kill -9 "${PROXY_PID}" 2>/dev/null
wait "${PROXY_PID}" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_105292"

# 139 = SIGSEGV: the original bug shape. Refuse it explicitly.
if [ "${CLIENT_EXIT}" = "139" ]; then
    echo "BAD: client segfaulted (exit ${CLIENT_EXIT})"
    head -50 "${CLIENT_LOG}"
    exit 1
fi
if grep -q "Address: 0x108\|Segmentation fault" "${CLIENT_LOG}"; then
    echo "BAD: crash signature found in client output (exit ${CLIENT_EXIT})"
    head -50 "${CLIENT_LOG}"
    exit 1
fi

# Require evidence that the send-error path was actually exercised. Without this,
# a test that completed the INSERT cleanly (proxy cutoff missed, buffer absorbed it)
# would silently pass and stop catching the bug. The post-fix shape is a non-zero
# exit plus a network-error marker in the client output.
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
