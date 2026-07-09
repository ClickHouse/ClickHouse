#!/usr/bin/env bash
# Tags: no-fasttest

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

# Start openssl s_server with port 0 to let it allocate a free port
# Use -www to serve a simple HTTP page (keeps connection open longer for complete handshake)
# Use -msg to capture more details if -tlsextdebug is not enough or format differs
SERVER_LOG="${CERT_DIR}/server.log"
openssl s_server -accept 0 -cert "${CERT_DIR}/server.crt" -key "${CERT_DIR}/server.key" -tlsextdebug -msg -www \
    > "${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

# Function to cleanup on exit
cleanup() {
    kill ${SERVER_PID} 2>/dev/null || true
    wait ${SERVER_PID} 2>/dev/null || true
    rm -rf "${CERT_DIR}"
}
trap cleanup EXIT

# Wait for the server to start and parse the port from the ACCEPT line
# The output will contain a line like "ACCEPT [::]:64701" or "ACCEPT 0.0.0.0:64701"
for _ in {1..20}; do
    if [ -f "${SERVER_LOG}" ]; then
        TEST_PORT=$(grep -oE 'ACCEPT .*:([0-9]+)' "${SERVER_LOG}" | head -1 | grep -oE '[0-9]+$')
        if [ -n "${TEST_PORT}" ]; then
            break
        fi
    fi
    sleep 0.5
done

if [ -z "${TEST_PORT}" ]; then
    echo "Failed to detect port from openssl s_server"
    cat "${SERVER_LOG}"
    exit 1
fi

# Test 1: Connect with clickhouse-client using --tls-sni-override
echo "Test 1: With --tls-sni-override=custom.example.com"

# Try to make a TLS connection with SNI override
# The TLS handshake will complete (which is what we're testing), but then the connection
# will hang because openssl s_server expects HTTP input. We timeout the client after 2 seconds.
# --accept-invalid-certificate is needed for the self-signed cert

CLIENT_LOG="${CERT_DIR}/client.log"
${CLICKHOUSE_CLIENT} --host 127.0.0.1 --port ${TEST_PORT} --secure \
    --tls-sni-override=custom.example.com --accept-invalid-certificate -q "SELECT 1" >"${CLIENT_LOG}" 2>&1 &
CLIENT_PID=$!

# Wait for TLS handshake to complete (should be very fast, but CI can be slow)
sleep 5

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
# We relax the grep to allow indentation and use -a for binary safety
if grep -a '[0-9a-f]\{4\} -' "${SERVER_LOG}" | sed 's/.*  //' | tr -d '\n' | grep -q 'custom\.example\.com'; then
    echo "PASS: custom.example.com found in TLS handshake"
else
    echo "FAIL: custom.example.com not found in TLS handshake"
    echo "Server Log:"
    cat "${SERVER_LOG}"
    echo "Client Log:"
    cat "${CLIENT_LOG}"
fi
