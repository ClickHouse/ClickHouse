#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs the secure ports (`https_port`) and `nc`.
# Tag no-parallel: `SYSTEM DROP CONNECTIONS CACHE` drops the server-wide connection pool.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `system.events` only lists events with a non-zero value, so `sum` over an empty set gives the
# 0 baseline of an event that has not fired yet.
function event()
{
    $CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM system.events WHERE event = '$1'"
}

# Runs the trigger until the event has grown. The events are server-wide and other tests can bump
# them concurrently, so only growth is ever asserted, never an exact value. Retrying also covers a
# single lookup or handshake being too fast to add up to a whole microsecond.
function expect_increase()
{
    local name=$1
    local trigger=$2
    local before
    before=$(event "$name")

    for _ in {1..10}
    do
        $trigger
        if [[ "$(event "$name")" -gt "$before" ]]
        then
            echo "$name increased"
            return
        fi
    done

    echo "$name did NOT increase, still $before"
}

# A host name that cannot resolve always reaches the resolver: a failure is not put into the DNS
# cache, so unlike a resolvable name this cannot be served from the cache instead.
function resolve_unresolvable_host()
{
    $CLICKHOUSE_CLIENT -q "SELECT * FROM remote('ThisHostNameDoesNotExistSoItShouldFail05038', system.one)" 2>/dev/null
}

# The address is a literal, so this exercises TLS and not DNS. Dropping the connection cache makes
# the connection really be established instead of taken from the pool. The server is both the
# client and the server of this handshake.
function https_request_to_self()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DROP CONNECTIONS CACHE"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM url('https://127.0.0.1:${CLICKHOUSE_PORT_HTTPS}/?query=SELECT%201', 'TSV', 'x UInt8') FORMAT Null"
}

# A request method longer than the 32 bytes the server accepts: the server answers
# `400 Bad Request` and closes the connection.
function malformed_http_request()
{
    printf 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA / HTTP/1.1\r\n\r\n' \
        | timeout 30 nc "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_HTTP}" > /dev/null
}

expect_increase DNSRequests resolve_unresolvable_host
expect_increase DNSRequestMicroseconds resolve_unresolvable_host

expect_increase TLSHandshakes https_request_to_self
expect_increase TLSHandshakeMicroseconds https_request_to_self
expect_increase TLSServerHandshakes https_request_to_self
expect_increase TLSServerHandshakeMicroseconds https_request_to_self

# A plaintext request to the secure port: the server cannot make sense of it as a `ClientHello`,
# so its side of the handshake fails.
function plaintext_request_to_https_port()
{
    printf 'GET / HTTP/1.1\r\n\r\n' \
        | timeout 30 nc "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_HTTPS}" > /dev/null
}

# A TLS handshake against the plain HTTP port: the peer answers with something that is not a
# `ServerHello`, so the client side of the handshake fails.
function https_request_to_plain_http_port()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DROP CONNECTIONS CACHE"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM url('https://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/', 'TSV', 'x UInt8') FORMAT Null" 2>/dev/null
}

expect_increase HTTPServerConnectionsErrors malformed_http_request

expect_increase TLSServerHandshakeErrors plaintext_request_to_https_port
expect_increase TLSHandshakeErrors https_request_to_plain_http_port
