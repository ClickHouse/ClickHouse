#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An explicit `--listen_host` on the command line must be a hard override of the
# `listen_host` entries in a loaded config file. Here the config lists a loopback
# address followed by an unroutable one (192.0.2.0/24 is RFC 5737 TEST-NET-1, never
# assigned to a local interface). Without the override, `getMultipleValuesFromConfig`
# would still return the unroutable `listen_host[1]` entry and binding it with
# `listen_try=0` would fail. With `--listen_host 127.0.0.1` the config hosts are
# ignored entirely, so the HTTP listener starts successfully.

# Fail fast: if the leaked `listen_host[1]` makes `SYSTEM START LISTEN HTTP` fail, `clickhouse-local`
# exits non-zero and the test must report that failure instead of swallowing it during cleanup.
set -e

CONFIG="${CLICKHOUSE_TMP}/04303_listen_override_config.xml"
trap 'rm -f "$CONFIG"' EXIT
cat > "$CONFIG" <<'XML'
<clickhouse>
    <listen_host>127.0.0.1</listen_host>
    <listen_host>192.0.2.1</listen_host>
</clickhouse>
XML

${CLICKHOUSE_LOCAL} \
    --config-file "$CONFIG" \
    --listen_host 127.0.0.1 \
    --tcp_port 0 \
    --http_port 0 \
    --query "
    SYSTEM START LISTEN HTTP;
    SELECT 'http_started';
    SYSTEM STOP LISTEN HTTP;
    SELECT 'http_stopped';
"
