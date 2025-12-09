#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Capture the server-reported port (body) and curl's actual local port (write-out).
# Note: with -w the local port is appended after the body on stdout; if curl fails, set -e makes the script exit with curl's code.
out=$(${CLICKHOUSE_CURL} -sS \
    -w '\nPORT:%{local_port}\n' \
    "${CLICKHOUSE_URL}&query_id=my_id&query=SELECT+port+FROM+system.processes+WHERE+query_id%3D%27my_id%27+ORDER+BY+elapsed+LIMIT+1")

# First line is the server-reported port; the line prefixed with PORT: is curl's own local port.
server_port=$(printf '%s\n' "$out" | head -n1)
client_port=$(printf '%s\n' "$out" | sed -n 's/^PORT://p' | head -n1)

[ "$server_port" = "$client_port" ] || { echo "ports differ: $server_port vs $client_port"; exit 1; }
echo "OK"
