#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires SSL support for cloud endpoint detection
# Tests that cloud endpoint auto-login detection doesn't override other auth methods

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test 1: Using --host= syntax with explicit credentials should not cause "option cannot be specified more than once" error
# This tests that when argv[1] starts with '-', we don't trigger cloud endpoint auto-detection
echo "Test 1: --host= with --secure should work"
output=$($CLICKHOUSE_CLIENT_BINARY --host="${CLICKHOUSE_HOST}" --port="${CLICKHOUSE_PORT_TCP}" --user=default --query "SELECT 1" 2>&1)
if echo "$output" | grep -q "cannot be specified more than once"; then
    echo "FAILED: duplicate option error"
else
    echo "OK"
fi

# Test 2: Using --host (space) syntax with explicit credentials should work
echo "Test 2: --host (space) with credentials should work"
output=$($CLICKHOUSE_CLIENT_BINARY --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_TCP}" --user default --query "SELECT 1" 2>&1)
if echo "$output" | grep -q "cannot be specified more than once"; then
    echo "FAILED: duplicate option error"
else
    echo "OK"
fi

# Test 3: Verify --host= with --port= doesn't cause connection string mixing error
echo "Test 3: --host= with --port= should work"
output=$($CLICKHOUSE_CLIENT_BINARY --host="${CLICKHOUSE_HOST}" --port="${CLICKHOUSE_PORT_TCP}" --query "SELECT 1" 2>&1)
if echo "$output" | grep -q "Mixing a connection string"; then
    echo "FAILED: connection string mixing error"
else
    echo "OK"
fi

# Test 4: Verify --host= with --port (space) doesn't cause connection string mixing error  
echo "Test 4: --host= with --port (space) should work"
output=$($CLICKHOUSE_CLIENT_BINARY --host="${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_TCP}" --query "SELECT 1" 2>&1)
if echo "$output" | grep -q "Mixing a connection string"; then
    echo "FAILED: connection string mixing error"
else
    echo "OK"
fi

# Test 5: Test with a config file containing credentials - should use config credentials
# Create a temporary config file with user/password
CONFIG_FILE="${CLICKHOUSE_TMP}/test_client_config_$$.xml"
cat > "$CONFIG_FILE" << EOF
<config>
    <user>default</user>
    <password></password>
</config>
EOF

echo "Test 5: Config file credentials should be respected"
output=$($CLICKHOUSE_CLIENT_BINARY --config-file="$CONFIG_FILE" --host="${CLICKHOUSE_HOST}" --port="${CLICKHOUSE_PORT_TCP}" --query "SELECT 1" 2>&1)
if echo "$output" | grep -qi "login\|OAuth\|browser"; then
    echo "FAILED: OAuth login triggered despite config credentials"
else
    echo "OK"
fi

rm -f "$CONFIG_FILE"

# Test 6: Multiple --host and --port combinations should all work
echo "Test 6: Multiple host/port format variations"
failed=0
for cmd in \
    "$CLICKHOUSE_CLIENT_BINARY --host=${CLICKHOUSE_HOST} --port=${CLICKHOUSE_PORT_TCP} --query 'SELECT 1'" \
    "$CLICKHOUSE_CLIENT_BINARY --host ${CLICKHOUSE_HOST} --port=${CLICKHOUSE_PORT_TCP} --query 'SELECT 1'" \
    "$CLICKHOUSE_CLIENT_BINARY --host=${CLICKHOUSE_HOST} --port ${CLICKHOUSE_PORT_TCP} --query 'SELECT 1'" \
    "$CLICKHOUSE_CLIENT_BINARY --host ${CLICKHOUSE_HOST} --port ${CLICKHOUSE_PORT_TCP} --query 'SELECT 1'"
do
    output=$(eval "$cmd" 2>&1)
    if echo "$output" | grep -qiE "error|BAD_ARGUMENTS|cannot be specified"; then
        echo "FAILED: $cmd"
        ((failed++))
    fi
done
if [ $failed -eq 0 ]; then
    echo "OK"
else
    echo "FAILED: $failed commands failed"
fi

echo "All tests completed"
