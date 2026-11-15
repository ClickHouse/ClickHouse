#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The client must not send `Ping` before a query. Pinging costs a round trip, and, worse, a `Pong`
# that does not arrive within `sync_request_timeout` is indistinguishable from a closed connection,
# so the client used to abandon a perfectly alive session - silently losing its temporary tables,
# current database and session settings.
#
# The check below is deterministic: all the traffic goes through a proxy that delays the beginning of
# every server response by two seconds, while the client is configured with `sync_request_timeout` of
# one second. A client that pings before a query cannot get the `Pong` in time, so it reconnects and
# loses the temporary table created by the first query. A client that does not ping is unaffected -
# the delay only makes the queries themselves slower, which no timeout here objects to.

# The names are unique per run, so that concurrent runs of this test do not interfere.
PROXY_SCRIPT="${CLICKHOUSE_TMP}/04651_proxy_${CLICKHOUSE_DATABASE}.py"
PROXY_PORT_FILE="${CLICKHOUSE_TMP}/04651_proxy_${CLICKHOUSE_DATABASE}.port"
CLIENT_CONFIG="${CLICKHOUSE_TMP}/04651_client_${CLICKHOUSE_DATABASE}.xml"

rm -f "$PROXY_PORT_FILE"

cat > "$PROXY_SCRIPT" <<'EOF'
import os
import socket
import sys
import threading
import time

backend_host = sys.argv[1]
backend_port = int(sys.argv[2])
delay = float(sys.argv[3])
port_file = sys.argv[4]

listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", 0))
listener.listen(16)

with open(port_file + ".tmp", "w") as f:
    f.write(str(listener.getsockname()[1]))
os.rename(port_file + ".tmp", port_file)


def pump(src, dst, state, from_server):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            if from_server:
                # Delay only the beginning of a response, not every piece of it.
                if state["awaiting_response"]:
                    state["awaiting_response"] = False
                    time.sleep(delay)
            else:
                state["awaiting_response"] = True
            dst.sendall(data)
    except OSError:
        pass
    for sock in (src, dst):
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


def serve(client):
    try:
        server = socket.create_connection((backend_host, backend_port))
    except OSError:
        client.close()
        return
    state = {"awaiting_response": False}
    threading.Thread(target=pump, args=(client, server, state, False), daemon=True).start()
    threading.Thread(target=pump, args=(server, client, state, True), daemon=True).start()


while True:
    connection, _ = listener.accept()
    threading.Thread(target=serve, args=(connection,), daemon=True).start()
EOF

python3 "$PROXY_SCRIPT" "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT_TCP" 2 "$PROXY_PORT_FILE" &
PROXY_PID=$!
# shellcheck disable=SC2064
trap "kill $PROXY_PID 2>/dev/null; rm -f '$PROXY_SCRIPT' '$PROXY_PORT_FILE' '$CLIENT_CONFIG'" EXIT

for _ in {1..300}; do
    [ -s "$PROXY_PORT_FILE" ] && break
    sleep 0.1
done

PROXY_PORT=$(cat "$PROXY_PORT_FILE")

echo '<clickhouse><sync_request_timeout>1</sync_request_timeout></clickhouse>' > "$CLIENT_CONFIG"

# The address of the proxy replaces the address of the server, the rest of the options is kept.
CLIENT_OPT=$(echo "${CLICKHOUSE_CLIENT_OPT}" | sed "s/--host=[^ ]*//g; s/--port=[^ ]*//g")

# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT_BINARY} ${CLIENT_OPT} --config-file "$CLIENT_CONFIG" --host 127.0.0.1 --port "$PROXY_PORT" --multiquery "
    CREATE TEMPORARY TABLE t_04651 (x UInt8);
    INSERT INTO t_04651 VALUES (1);
    SELECT x FROM t_04651;
"
