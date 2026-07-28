#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Some queries desynchronize the protocol: the client sends the query followed by the (empty) block
# of external data, the server throws before it reads that block, and then it closes the connection
# with `Unexpected packet Data received from client`. This happens for every `serverError` hint in a
# `.sql` test, so the client recovers by reconnecting after an expected error.
#
# The recovery must not depend on the close having already arrived - a non-blocking check of the
# socket cannot see a connection that the server has just closed. The client has to ask the server
# and wait for the answer, so that everything the server has already sent is observed.
#
# The check below is deterministic: all the traffic goes through a proxy that delays everything the
# server sends, the close included. A client that only looks at the socket does not notice that the
# connection is gone and fails the next query with `ATTEMPT_TO_READ_AFTER_EOF`.

# The names are unique per run, so that concurrent runs of this test do not interfere.
PROXY_SCRIPT="${CLICKHOUSE_TMP}/04655_proxy_${CLICKHOUSE_DATABASE}.py"
PROXY_PORT_FILE="${CLICKHOUSE_TMP}/04655_proxy_${CLICKHOUSE_DATABASE}.port"

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


def pump(src, dst, from_server):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            if from_server:
                time.sleep(delay)
            dst.sendall(data)
    except OSError:
        pass
    # The close is delayed as well: this is what makes a check of the socket alone insufficient.
    if from_server:
        time.sleep(delay)
    try:
        dst.shutdown(socket.SHUT_WR)
    except OSError:
        pass


def serve(client):
    try:
        server = socket.create_connection((backend_host, backend_port))
    except OSError:
        client.close()
        return
    threading.Thread(target=pump, args=(client, server, False), daemon=True).start()
    threading.Thread(target=pump, args=(server, client, True), daemon=True).start()


while True:
    connection, _ = listener.accept()
    threading.Thread(target=serve, args=(connection,), daemon=True).start()
EOF

python3 "$PROXY_SCRIPT" "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT_TCP" 1 "$PROXY_PORT_FILE" &
PROXY_PID=$!
# shellcheck disable=SC2064
trap "kill $PROXY_PID 2>/dev/null; rm -f '$PROXY_SCRIPT' '$PROXY_PORT_FILE'" EXIT

for _ in {1..300}; do
    [ -s "$PROXY_PORT_FILE" ] && break
    sleep 0.1
done

PROXY_PORT=$(cat "$PROXY_PORT_FILE")

# The address of the proxy replaces the address of the server, the rest of the options is kept.
CLIENT_OPT=$(echo "${CLICKHOUSE_CLIENT_OPT}" | sed "s/--host=[^ ]*//g; s/--port=[^ ]*//g")

# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT_BINARY} ${CLIENT_OPT} --host 127.0.0.1 --port "$PROXY_PORT" --multiquery "
    CREATE DATABASE ${CLICKHOUSE_DATABASE}_04655 SETTINGS x = 1; -- { serverError UNKNOWN_SETTING }
    SELECT 'The connection is usable after an expected error';
"
