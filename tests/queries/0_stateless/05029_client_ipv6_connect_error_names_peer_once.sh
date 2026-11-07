#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A closed port on the IPv6 loopback fails on the deferred path (`connect` returns `EINPROGRESS`, the
# refusal arrives through `SO_ERROR`), and the socket error names the peer as `[::1]:1`. The client
# must recognize its own endpoint in that bracketed form and not append a second `(::1:1)` copy of it.
CLIENT_OPT=$(echo "${CLICKHOUSE_CLIENT_OPT}" | sed "s/--host=[^ ]*//g; s/--port=[^ ]*//g")

# shellcheck disable=SC2086
error="$(${CLICKHOUSE_CLIENT_BINARY} ${CLIENT_OPT} --host "::1" --port 1 --connect_timeout 1 --query "SELECT 1" 2>&1 > /dev/null)"

# The peer is named once, by the socket error itself; the client adds no `(::1:1)` copy of it.
echo "${error}" | grep -Fc '[::1]:1'
echo "${error}" | grep -Fc '(::1:1' || true
