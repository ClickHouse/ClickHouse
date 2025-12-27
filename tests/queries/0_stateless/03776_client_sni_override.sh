#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# This test verifies that the --tls-sni-override option correctly sets the SNI field in TLS connections

# Generate a self-signed certificate for testing
CERT_DIR="${CLICKHOUSE_TMP}/sni_test_$$"
mkdir -p "${CERT_DIR}"

# Generate a private key and self-signed certificate
openssl req -newkey rsa:2048 -nodes -keyout "${CERT_DIR}/server.key" -x509 -days 1 -out "${CERT_DIR}/server.crt" \
    -subj "/CN=localhost" 2>/dev/null >/dev/null

# Find an available port
TEST_PORT=19444
while netstat -an 2>/dev/null | grep -q ":${TEST_PORT} "; do
    TEST_PORT=$((TEST_PORT + 1))
done

# Start openssl s_server in the background with -tlsextdebug to capture SNI
# Use -www to serve a simple HTTP page (keeps connection open longer for complete handshake)
SERVER_LOG="${CERT_DIR}/server.log"
openssl s_server -accept ${TEST_PORT} -cert "${CERT_DIR}/server.crt" -key "${CERT_DIR}/server.key" -tlsextdebug -www \
    > "${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

# Function to cleanup on exit
cleanup() {
    kill ${SERVER_PID} 2>/dev/null || true
    wait ${SERVER_PID} 2>/dev/null || true
    rm -rf "${CERT_DIR}"
}
trap cleanup EXIT

# Give the server time to start
sleep 2

# Test 1: Connect with clickhouse-client using --tls-sni-override
echo "Test 1: With --tls-sni-override=custom.example.com"

# Try to make a TLS connection with SNI override
# The TLS handshake will complete (which is what we're testing), but then the connection
# will hang because openssl s_server expects HTTP input. We timeout the client after 2 seconds.
# --accept-invalid-certificate is needed for the self-signed cert

${CLICKHOUSE_CLIENT} --host 127.0.0.1 --port ${TEST_PORT} --secure \
    --tls-sni-override=custom.example.com --accept-invalid-certificate -q "SELECT 1" >/dev/null 2>&1 &
CLIENT_PID=$!

# Wait for TLS handshake to complete (should be very fast)
sleep 2

# Kill the client (it will be hung waiting for response from openssl)
kill -9 ${CLIENT_PID} 2>/dev/null || true
wait ${CLIENT_PID} 2>/dev/null || true

# Kill the openssl server to force it to flush its output
kill ${SERVER_PID} 2>/dev/null || true
wait ${SERVER_PID} 2>/dev/null || true

# Give time for final output to be written
sleep 2

# Check if SNI was sent - extract just the ASCII representation column from the hex dump
# Format: "0010 - 70 6c 65 2e 63 6f 6d                              ple.com"
# We extract everything after the last 3+ spaces, concatenate, and search
if grep '^[0-9a-f]\{4\} -' "${SERVER_LOG}" | sed 's/.*   //' | tr -d '\n' | grep -q 'custom\.example\.com'; then
    echo "PASS: custom.example.com found in TLS handshake"
else
    echo "FAIL: custom.example.com not found in TLS handshake"
    cat "${SERVER_LOG}"
fi
