#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `--tcp_port` / `--http_port` are range-checked when parsed on the command line, but a `tcp_port` /
# `http_port` value coming from a loaded config file reaches the listener path unchecked. `createServer`
# casts the configured port to `UInt16`, so an out-of-range config value such as `70000` would silently
# wrap (to `4464`) and `SYSTEM START LISTEN` would bind an unexpected port. The effective port must be
# validated before binding. The port is supplied only via the config file (no CLI `--tcp_port` /
# `--http_port` override) so this exercises the config-file path specifically, not the CLI validation.

CONFIG="${CLICKHOUSE_TMP}/04339_config_port_range.xml"
trap 'rm -f "$CONFIG"' EXIT

# Writes a config file with the given port setting, runs the listener command itself (not via a
# command substitution, so the exit status is not discarded), and asserts it was rejected with the
# out-of-range diagnostic. Returns nonzero on unexpected acceptance or a wrong error so the script
# fails hard if the config-file port validation regresses.
check_rejected() {
    local desc="$1"
    local port_xml="$2"
    local listen="$3"
    cat > "$CONFIG" <<XML
<clickhouse>
    ${port_xml}
</clickhouse>
XML
    local out
    if out=$($CLICKHOUSE_LOCAL --config-file "$CONFIG" --query "$listen" 2>&1); then
        echo "FAIL: $desc was unexpectedly accepted: $out"
        return 1
    fi
    if echo "$out" | grep -qF "a port number must be in the range 0..65535"; then
        echo "rejected: $desc"
    else
        echo "FAIL: $desc was not rejected with the expected error: $out"
        return 1
    fi
}

rc=0
check_rejected "config tcp_port=70000"  '<tcp_port>70000</tcp_port>'    'SYSTEM START LISTEN TCP'  || rc=1
check_rejected "config tcp_port=-1"      '<tcp_port>-1</tcp_port>'      'SYSTEM START LISTEN TCP'  || rc=1
check_rejected "config http_port=99999"  '<http_port>99999</http_port>' 'SYSTEM START LISTEN HTTP' || rc=1
check_rejected "config http_port=-5"     '<http_port>-5</http_port>'    'SYSTEM START LISTEN HTTP' || rc=1

# A valid config-file port is not rejected and the listener binds. Use port 0 (OS-assigned) with a
# single explicit `--listen_host` so the bind is deterministic and collision-free.
cat > "$CONFIG" <<XML
<clickhouse>
    <tcp_port>0</tcp_port>
</clickhouse>
XML
$CLICKHOUSE_LOCAL --config-file "$CONFIG" --listen_host 127.0.0.1 --query "
    SYSTEM START LISTEN TCP;
    SELECT 'accepted: config tcp_port=0';
    SYSTEM STOP LISTEN TCP;
" || rc=1

exit $rc
