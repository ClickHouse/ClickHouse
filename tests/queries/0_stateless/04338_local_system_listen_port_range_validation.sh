#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `--tcp_port` / `--http_port` define the port that `SYSTEM START LISTEN` will bind, and the
# listener path stores them as `UInt16`. Out-of-range values must be rejected up front instead
# of silently wrapping (without validation `-1` would become `65535` and `70000` would become
# `4464`, exposing the listener on an unexpected port). `0` (OS-assigned) and `65535` are valid.

# Returns nonzero (so the script can fail hard) unless the value was rejected with the expected
# out-of-range diagnostic. Without this, a build that silently accepts an invalid port would still
# leave a zero exit status, so only the reference diff would catch the regression.
check_rejected() {
    local out
    if out=$($CLICKHOUSE_LOCAL "$@" --query "SELECT 'should_not_run'" 2>&1); then
        echo "FAIL: unexpectedly accepted: $*"
        return 1
    fi
    if echo "$out" | grep -qF "a port number must be in the range 0..65535"; then
        echo "rejected: $*"
    else
        echo "FAIL: wrong error for $*: $out"
        return 1
    fi
}

rc=0
check_rejected --tcp_port=-1 || rc=1
check_rejected --tcp_port=70000 || rc=1
check_rejected --http_port=-1 || rc=1
check_rejected --http_port=99999 || rc=1

# Valid boundary values are accepted (setting the port without SYSTEM START LISTEN is harmless).
$CLICKHOUSE_LOCAL --tcp_port=0 --http_port=0 --query "SELECT 'accepted: tcp=0 http=0'" || rc=1
$CLICKHOUSE_LOCAL --tcp_port=65535 --http_port=65535 --query "SELECT 'accepted: tcp=65535 http=65535'" || rc=1

exit $rc
