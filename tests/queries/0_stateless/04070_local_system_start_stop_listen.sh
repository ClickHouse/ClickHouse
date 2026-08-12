#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that clickhouse-local supports SYSTEM START/STOP LISTEN
# and that the HTTP listener is actually reachable after starting.
#
# Use OS-assigned ports (`--tcp_port 0 --http_port 0`) to avoid collisions with
# parallel CI jobs, then construct the `url()` endpoint from the actual bound
# port discovered via `getServerPort('http_port')`.
$CLICKHOUSE_LOCAL \
    --listen_host 127.0.0.1 \
    --tcp_port 0 \
    --http_port 0 \
    --query "
    SYSTEM START LISTEN TCP;
    SELECT 'tcp_started';
    SYSTEM START LISTEN HTTP;
    SELECT 'http_started';
    SELECT * FROM url('http://127.0.0.1:' || toString(getServerPort('http_port')) || '/?query=SELECT+1', LineAsString) FORMAT Null;
    SELECT 'http_verified';
    SYSTEM STOP LISTEN TCP;
    SELECT 'tcp_stopped';
    SYSTEM STOP LISTEN HTTP;
    SELECT 'http_stopped';
"
