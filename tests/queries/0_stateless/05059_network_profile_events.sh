#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs the secure ports (`https_port`) and `nc`.
# Tag no-parallel: `SYSTEM DROP DNS CACHE` and `SYSTEM DROP CONNECTIONS CACHE` drop server-wide caches.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reads all the requested events in a single query: `system.events` only lists events with a
# non-zero value, so `sumIf` over an empty set gives the 0 baseline of an event that has not fired
# yet. One query per check instead of one per event keeps the test fast under sanitizers, where
# starting the client dominates the run time.
function read_events()
{
    local exprs=""
    local name
    for name in "$@"
    do
        exprs+="sumIf(value, event = '$name'), "
    done
    $CLICKHOUSE_CLIENT -q "SELECT ${exprs%, } FROM system.events"
}

# Runs the trigger until every event has grown. The events are server-wide and other tests can bump
# them concurrently, so only growth is ever asserted, never an exact value. Retrying also covers a
# single lookup or handshake being too fast to add up to a whole microsecond.
function expect_increase()
{
    local trigger=$1
    shift
    local names=("$@")
    local before=() after=()
    local i

    read -r -a before <<< "$(read_events "${names[@]}")"
    after=("${before[@]}")

    for _ in {1..10}
    do
        $trigger
        read -r -a after <<< "$(read_events "${names[@]}")"

        local all_increased=1
        for i in "${!names[@]}"
        do
            [[ "${after[$i]}" -gt "${before[$i]}" ]] || all_increased=0
        done
        [[ "$all_increased" -eq 1 ]] && break
    done

    for i in "${!names[@]}"
    do
        if [[ "${after[$i]}" -gt "${before[$i]}" ]]
        then
            echo "${names[$i]} increased"
        else
            echo "${names[$i]} did NOT increase, still ${before[$i]}"
        fi
    done
}

# Dropping the DNS cache makes the host name be resolved for real instead of being served from the
# cache, so a resolution is guaranteed to happen.
function resolve_host()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DROP DNS CACHE; SELECT * FROM remote('localhost', system.one) FORMAT Null"
}

# A host name that cannot resolve is never served from the cache, because a failure is not put into
# it. The name has a label longer than the 63 bytes a DNS label may have, so the system resolver
# rejects it locally and returns immediately; a name that is merely absent from the zone would
# instead cost a resolver timeout on every attempt, which is tens of seconds on a machine without a
# reachable resolver. Locally rejected names are within the contract of `DNSRequests`, which counts
# name resolution attempts rather than packets sent.
UNRESOLVABLE_HOST="$(printf 'a%.0s' {1..70}).invalid"

function resolve_unresolvable_host()
{
    $CLICKHOUSE_CLIENT -q "SELECT * FROM remote('${UNRESOLVABLE_HOST}', system.one)" 2>/dev/null
}

# The address is a literal, so this exercises TLS and not DNS. Dropping the connection cache makes
# the connection really be established instead of taken from the pool. The server is both the
# client and the server of this handshake.
function https_request_to_self()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DROP CONNECTIONS CACHE; SELECT * FROM url('https://127.0.0.1:${CLICKHOUSE_PORT_HTTPS}/?query=SELECT%201', 'TSV', 'x UInt8') FORMAT Null"
}

# A request method longer than the 32 bytes the server accepts: the server answers
# `400 Bad Request` and closes the connection.
function malformed_http_request()
{
    printf 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA / HTTP/1.1\r\n\r\n' \
        | timeout 30 nc "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_HTTP}" > /dev/null
}

# A plaintext request to the secure port: the server cannot make sense of it as a `ClientHello`,
# so its side of the handshake fails.
function plaintext_request_to_https_port()
{
    printf 'GET / HTTP/1.1\r\n\r\n' \
        | timeout 30 nc "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_HTTPS}" > /dev/null
}

# A TLS handshake against the plain HTTP port: the peer answers with something that is not a
# `ServerHello`, so the client side of the handshake fails. `http_max_tries = 1` is essential:
# with the default of 10 the failing handshake is retried with an exponential backoff, which takes
# more than two minutes and by itself made this test time out.
function https_request_to_plain_http_port()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DROP CONNECTIONS CACHE; SELECT * FROM url('https://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/', 'TSV', 'x UInt8') FORMAT Null SETTINGS http_max_tries = 1" 2>/dev/null
}

expect_increase resolve_host DNSRequests DNSRequestMicroseconds
expect_increase resolve_unresolvable_host DNSRequestError

expect_increase https_request_to_self TLSHandshakes TLSHandshakeMicroseconds TLSServerHandshakes TLSServerHandshakeMicroseconds

expect_increase malformed_http_request HTTPServerConnectionsErrors

expect_increase plaintext_request_to_https_port TLSServerHandshakeErrors
expect_increase https_request_to_plain_http_port TLSHandshakeErrors
