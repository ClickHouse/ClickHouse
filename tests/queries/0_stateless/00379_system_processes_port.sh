#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

tmpdir="$(mktemp -d "${CURDIR}/00379_system_processes_port.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT

query_id="my_id_$(random_str 8)"

# Capture the server-reported port (in the http_response) and curl's actual local port (writeout).
set +e
${CLICKHOUSE_CURL} --fail-with-body -sS \
    -w 'PORT:%{local_port}\n' \
    "${CLICKHOUSE_URL}&query_id=${query_id}&query=SELECT+port+FROM+system.processes+WHERE+query_id%3D%27${query_id}%27+ORDER+BY+elapsed+LIMIT+1" \
    -o "$tmpdir/http_response" >"$tmpdir/writeout" 2>"$tmpdir/curl_stderr"
curl_status=$?
set -e

if [[ $curl_status -ne 0 ]]; then
    echo "curl failed (exit $curl_status)"
    cat "$tmpdir/curl_stderr" "$tmpdir/http_response"
    exit 1
fi

# http_response contains the server port; writeout has curl's own local port prefixed with PORT:.
server_port=$(head -n1 "$tmpdir/http_response")
client_port=$(sed -n 's/^PORT://p' "$tmpdir/writeout" | head -n1)

if ! [[ "$server_port" =~ ^[0-9]+$ ]]; then
    echo "unexpected server response: $server_port"
    exit 1
fi

if [[ "$server_port" != "$client_port" ]]; then
    echo "ports differ: $server_port vs $client_port"
    exit 1
fi
echo "OK"
