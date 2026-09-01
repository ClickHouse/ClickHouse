#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The HTTP handler factory is built from `<http_handlers>` while the listener socket is being bound, so
# a rejected handler rule used to surface as a bind failure: as `Listen [host]:port failed: <cause>`
# re-coded to `NETWORK_ERROR`, or, with `listen_try` set, as the unrelated advice to check `listen_host`
# and `http_port`. The handler configuration must be reported on its own terms.
#
# `http_port` is 0 (OS-assigned) with a single explicit `listen_host`, so the arms that do reach the
# bind cannot collide with a concurrently running copy of this test.

CONFIG="${CLICKHOUSE_TMP}/04935_local_http_handler_config.xml"
trap 'rm -f "$CONFIG"' EXIT

BAD_RULE='<http_handlers>
        <rule>
            <url>/bad</url>
            <methods>GET</methods>
            <handler>
                <type>no_such_handler_type</type>
            </handler>
        </rule>
        <defaults/>
    </http_handlers>'

GOOD_RULE='<http_handlers>
        <rule>
            <url>/ok</url>
            <methods>GET</methods>
            <handler>
                <type>static</type>
                <response_content>ok</response_content>
            </handler>
        </rule>
        <defaults/>
    </http_handlers>'

write_config() {
    cat > "$CONFIG" <<XML
<clickhouse>
    <listen_host>127.0.0.1</listen_host>
    <http_port>0</http_port>
    ${1}
    ${2}
</clickhouse>
XML
}

rc=0

# Runs the listener command and asserts it was rejected naming the handler rule, and that none of the
# listener-oriented wordings appear. The command runs directly rather than in a command substitution
# under `if`, so a zero exit status (an unexpected successful start) is caught rather than discarded.
check_handler_error_reported() {
    local desc="$1"
    local out
    if out=$($CLICKHOUSE_LOCAL --config-file "$CONFIG" --query 'SYSTEM START LISTEN HTTP' 2>&1); then
        echo "FAIL: $desc unexpectedly started: $out"
        return 1
    fi
    if ! echo "$out" | grep -qF "Unknown handler type 'no_such_handler_type'"; then
        echo "FAIL: $desc did not name the handler type: $out"
        return 1
    fi
    # Asserted on the reported message itself, so unrelated output cannot satisfy it either way.
    # `grep -F` because the wording that has to be absent contains regex metacharacters.
    for absent in 'Listen [' 'NETWORK_ERROR' 'check listen_host and http_port'; do
        if echo "$out" | grep -qF "$absent"; then
            echo "FAIL: $desc still reported as a listener failure ($absent): $out"
            return 1
        fi
    done
    echo "reported: $desc"
}

write_config "$BAD_RULE" ''
check_handler_error_reported 'rejected handler rule' || rc=1

# `listen_try` reaches the other exit of the same code path: the failure was logged as a warning about
# `<listen_host>` and the listener silently skipped, after which the per-protocol verification blamed
# `listen_host` and `http_port`.
write_config "$BAD_RULE" '<listen_try>1</listen_try>'
check_handler_error_reported 'rejected handler rule with listen_try' || rc=1

# A valid `<http_handlers>` section still binds, in both `listen_try` settings. Without this the arms
# above would also pass against a build that refuses to start the HTTP listener for any reason.
write_config "$GOOD_RULE" ''
$CLICKHOUSE_LOCAL --config-file "$CONFIG" --query "
    SYSTEM START LISTEN HTTP;
    SELECT 'accepted: valid handler rule';
    SYSTEM STOP LISTEN HTTP;
" || rc=1

write_config "$GOOD_RULE" '<listen_try>1</listen_try>'
$CLICKHOUSE_LOCAL --config-file "$CONFIG" --query "
    SYSTEM START LISTEN HTTP;
    SELECT 'accepted: valid handler rule with listen_try';
    SYSTEM STOP LISTEN HTTP;
" || rc=1

exit $rc
