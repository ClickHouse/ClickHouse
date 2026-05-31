#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: --jwt-command requires a build with JWT and SSL support

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Well-formed JWT with far-future exp; server will reject, but the client must accept its shape.
SAMPLE_JWT="eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTksInN1YiI6InRlc3QifQ.fake"

echo "Test 1: --jwt and --jwt-command together should give BAD_ARGUMENTS"
output=$($CLICKHOUSE_CLIENT_BINARY --jwt "$SAMPLE_JWT" --jwt-command "echo $SAMPLE_JWT" --query "SELECT 1" 2>&1)
if echo "$output" | grep -qi "cannot both be specified\|BAD_ARGUMENTS"; then
    echo "OK"
else
    echo "FAILED: expected BAD_ARGUMENTS, got: $output"
fi

echo "Test 2: --jwt-command with non-default --user should give BAD_ARGUMENTS"
output=$($CLICKHOUSE_CLIENT_BINARY --user alice --jwt-command "echo $SAMPLE_JWT" --query "SELECT 1" 2>&1)
if echo "$output" | grep -qi "User and JWT flags\|BAD_ARGUMENTS"; then
    echo "OK"
else
    echo "FAILED: expected BAD_ARGUMENTS, got: $output"
fi

echo "Test 3: --jwt-command with --login should give BAD_ARGUMENTS"
output=$($CLICKHOUSE_CLIENT_BINARY --login=device --jwt-command "echo $SAMPLE_JWT" --query "SELECT 1" 2>&1)
if echo "$output" | grep -qi "cannot both be specified\|BAD_ARGUMENTS"; then
    echo "OK"
else
    echo "FAILED: expected BAD_ARGUMENTS, got: $output"
fi

echo "Test 4: --jwt-command with empty stdout should fail with AUTHENTICATION_FAILED"
output=$($CLICKHOUSE_CLIENT_BINARY --jwt-command "true" --query "SELECT 1" 2>&1)
if echo "$output" | grep -qi "empty output.*AUTHENTICATION_FAILED\|AUTHENTICATION_FAILED.*empty output"; then
    echo "OK"
else
    echo "FAILED: expected AUTHENTICATION_FAILED for empty output, got: $output"
fi

echo "Test 5: --jwt-command exiting with non-zero status should fail with AUTHENTICATION_FAILED"
output=$($CLICKHOUSE_CLIENT_BINARY --jwt-command "exit 42" --query "SELECT 1" 2>&1)
if echo "$output" | grep -qi "non-zero status 42.*AUTHENTICATION_FAILED\|AUTHENTICATION_FAILED.*non-zero status 42"; then
    echo "OK"
else
    echo "FAILED: expected AUTHENTICATION_FAILED with retcode 42, got: $output"
fi

echo "Test 6: --jwt-command stderr should be forwarded to client stderr"
MARKER="forwarded-from-script-stderr"
output=$($CLICKHOUSE_CLIENT_BINARY --jwt-command "echo $MARKER 1>&2; echo $SAMPLE_JWT" --host localhost --port 1 --query "SELECT 1" 2>&1)
if echo "$output" | grep -q "$MARKER"; then
    echo "OK"
else
    echo "FAILED: expected stderr marker '$MARKER' in output, got: $output"
fi

echo "Test 7: --jwt-command-timeout kills a hanging script"
start=$SECONDS
output=$($CLICKHOUSE_CLIENT_BINARY --jwt-command "sleep 30; echo $SAMPLE_JWT" --jwt-command-timeout 1 --query "SELECT 1" 2>&1)
elapsed=$((SECONDS - start))
if echo "$output" | grep -qi "timed out after 1 seconds.*AUTHENTICATION_FAILED\|AUTHENTICATION_FAILED.*timed out after 1 seconds" && [ "$elapsed" -lt 10 ]; then
    echo "OK"
else
    echo "FAILED: expected AUTHENTICATION_FAILED timeout under 10s, elapsed=${elapsed}s, got: $output"
fi

echo "Test 8: --jwt-command-timeout=0 should be rejected"
output=$($CLICKHOUSE_CLIENT_BINARY --jwt-command "echo $SAMPLE_JWT" --jwt-command-timeout 0 --query "SELECT 1" 2>&1)
if echo "$output" | grep -qi "must be positive.*BAD_ARGUMENTS\|BAD_ARGUMENTS.*must be positive"; then
    echo "OK"
else
    echo "FAILED: expected BAD_ARGUMENTS for non-positive timeout, got: $output"
fi

echo "Test 9: --jwt-command is actually executed"
MARKER_FILE="${CLICKHOUSE_TMP}/04206_jwt_command_marker_$$"
rm -f "$MARKER_FILE"
$CLICKHOUSE_CLIENT_BINARY --jwt-command "echo ran > '$MARKER_FILE'; echo $SAMPLE_JWT" --host localhost --port 1 --query "SELECT 1" > /dev/null 2>&1
if [ -f "$MARKER_FILE" ]; then
    echo "OK"
else
    echo "FAILED: marker file not created, command did not run"
fi
rm -f "$MARKER_FILE"

echo "Test 10: --jwt-command-timeout from XML config file takes effect"
CFG="${CLICKHOUSE_TMP}/04206_jwt_command_cfg_$$.xml"
cat > "$CFG" <<EOF
<config>
    <jwt-command-timeout>1</jwt-command-timeout>
</config>
EOF
start=$SECONDS
output=$($CLICKHOUSE_CLIENT_BINARY --config-file "$CFG" --jwt-command "sleep 30; echo $SAMPLE_JWT" --query "SELECT 1" 2>&1)
elapsed=$((SECONDS - start))
if echo "$output" | grep -qi "timed out after 1 seconds" && [ "$elapsed" -lt 10 ]; then
    echo "OK"
else
    echo "FAILED: expected timeout under 10s from XML config, elapsed=${elapsed}s, got: $output"
fi
rm -f "$CFG"

echo "Test 11: stdin-reading script completes promptly (stdin is closed)"
# If the child's stdin is closed by the parent, 'read X' returns immediately on EOF and
# the JWT is echoed before the 1s watchdog fires. If stdin were left open, 'read X' would
# block and the watchdog would surface 'timed out after 1 seconds'. We assert on that
# message rather than wall-clock time so the test is not flaky under loaded CI runs.
output=$($CLICKHOUSE_CLIENT_BINARY --jwt-command "read X; echo $SAMPLE_JWT" --jwt-command-timeout 1 --host localhost --port 1 --query "SELECT 1" 2>&1)
if echo "$output" | grep -qi "timed out"; then
    echo "FAILED: jwt-command child's stdin was not closed (got: $output)"
else
    echo "OK"
fi

echo "Test 12: CLI --jwt-command-timeout overrides XML config"
CFG="${CLICKHOUSE_TMP}/04206_jwt_command_cfg_override_$$.xml"
cat > "$CFG" <<EOF
<config>
    <jwt-command-timeout>30</jwt-command-timeout>
</config>
EOF
start=$SECONDS
output=$($CLICKHOUSE_CLIENT_BINARY --config-file "$CFG" --jwt-command "sleep 30; echo $SAMPLE_JWT" --jwt-command-timeout 1 --query "SELECT 1" 2>&1)
elapsed=$((SECONDS - start))
if echo "$output" | grep -qi "timed out after 1 seconds" && [ "$elapsed" -lt 10 ]; then
    echo "OK"
else
    echo "FAILED: expected CLI(1) to override XML(30), elapsed=${elapsed}s, got: $output"
fi
rm -f "$CFG"

echo "All tests completed"
