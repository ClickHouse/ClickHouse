#!/usr/bin/env bash

# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that clickhouse-local supports SYSTEM START/STOP LISTEN
# and that the HTTP listener is actually reachable after starting.

LOCAL_TCP_PORT=$((RANDOM % 10000 + 20000))
LOCAL_HTTP_PORT=$((RANDOM % 10000 + 30000))

# Use a single clickhouse-local invocation.
# After SYSTEM START LISTEN HTTP, verify the HTTP endpoint works
# by querying it via the url() table function from within the same process.
$CLICKHOUSE_LOCAL \
    --listen_host 127.0.0.1 \
    --tcp_port "$LOCAL_TCP_PORT" \
    --http_port "$LOCAL_HTTP_PORT" \
    --query "
    SYSTEM START LISTEN TCP;
    SELECT 'tcp_started';
    SYSTEM START LISTEN HTTP;
    SELECT 'http_started';
    SELECT * FROM url('http://127.0.0.1:$LOCAL_HTTP_PORT/?query=SELECT+1', LineAsString) FORMAT Null;
    SELECT 'http_verified';
    SYSTEM STOP LISTEN TCP;
    SELECT 'tcp_stopped';
    SYSTEM STOP LISTEN HTTP;
    SELECT 'http_stopped';
"
