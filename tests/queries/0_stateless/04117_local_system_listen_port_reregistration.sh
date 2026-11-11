#!/usr/bin/env bash

# Test that `getServerPort` reflects the *current* ephemeral port after a
# stop+start cycle in `clickhouse-local`.
#
# Background: with `--http_port 0`, each `SYSTEM START LISTEN HTTP` binds a
# fresh OS-assigned port. The internal port registry must be updated on every
# successful start so that `getServerPort('http_port')` returns the active
# port instead of a stale, closed one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# After STOP + START, the new port must be reachable via the value reported by
# `getServerPort`. If the registry returned a stale port from the first bind,
# the inner `url(...)` call would fail because the listener on that port was
# closed.
$CLICKHOUSE_LOCAL \
    --listen_host 127.0.0.1 \
    --http_port 0 \
    --query "
    SYSTEM START LISTEN HTTP;
    SYSTEM STOP LISTEN HTTP;
    SYSTEM START LISTEN HTTP;
    SELECT * FROM url('http://127.0.0.1:' || toString(getServerPort('http_port')) || '/?query=SELECT+1', LineAsString) FORMAT Null;
    SELECT 'reregistration_ok';
"
