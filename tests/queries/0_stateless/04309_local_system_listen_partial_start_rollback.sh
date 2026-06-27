#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A partial failure during `SYSTEM START LISTEN` must be all-or-nothing: when one
# `listen_host` binds successfully but a later one fails, the command has to report
# the failure (NETWORK_ERROR) and roll back the listener it already started, instead
# of leaving a port open or hanging.
#
# The config lists a bindable loopback host followed by an unroutable one
# (192.0.2.0/24 is RFC 5737 TEST-NET-1, never assigned to a local interface), so the
# bind on the second host always fails. With an explicit (config) `listen_host` list,
# `listen_try` is off, so that failure surfaces as NETWORK_ERROR rather than a warning.
# A fixed `tcp_port` is required because port 0 with multiple `listen_host` values is
# rejected up front; if that port happens to be in use by a parallel test the first
# bind fails too and the command still throws NETWORK_ERROR, so the assertion holds
# regardless of which bind fails. The point of the test is that the rollback path runs
# without deadlocking or hanging — `startServers` is invoked while holding
# `servers_lock`, so the cleanup must not re-acquire it.

set -e

CONFIG="${CLICKHOUSE_TMP}/04309_partial_failure_config.xml"
cat > "$CONFIG" <<'XML'
<clickhouse>
    <listen_host>127.0.0.1</listen_host>
    <listen_host>192.0.2.1</listen_host>
</clickhouse>
XML

$CLICKHOUSE_LOCAL --config-file "$CONFIG" --tcp_port 27183 \
    --query "SYSTEM START LISTEN TCP; -- { serverError NETWORK_ERROR }"

echo 'partial_start_rolled_back_ok'

rm -f "$CONFIG"
