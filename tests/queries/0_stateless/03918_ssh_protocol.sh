#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test SSH protocol support: connect via SSH and run queries.

# Check that the SSH port is configured and listening
if ! echo "" | nc -w 1 "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_SSH}" >/dev/null 2>&1; then
    echo "@@SKIP@@: SSH port ${CLICKHOUSE_PORT_SSH} is not available, skipping test"
    exit 0
fi

# Check that the ssh client is available
if ! command -v ssh &>/dev/null; then
    echo "@@SKIP@@: ssh client is not available, skipping test"
    exit 0
fi

# Copy the private key to a temp file with restrictive permissions,
# because OpenSSH refuses keys with permissions that are too open (644 from git).
SSH_USER_KEY_ORIG="$CURDIR/../../config/ssh_user_ed25519_key"
SSH_USER_KEY="${CLICKHOUSE_TMP}/ssh_user_ed25519_key_${CLICKHOUSE_TEST_UNIQUE_NAME}"
cp "$SSH_USER_KEY_ORIG" "$SSH_USER_KEY"
chmod 600 "$SSH_USER_KEY"

SSH_STDERR="${CLICKHOUSE_TMP}/ssh_stderr_${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Extract the base64 public key from the .pub file
SSH_USER_PUBKEY=$(awk '{print $2}' "$SSH_USER_KEY_ORIG.pub")

SSH_USER="ssh_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Common SSH options: disable host key checking, use BatchMode to fail fast
# instead of hanging on password prompt, and set timeouts.
SSH_OPTS=(
    -o StrictHostKeyChecking=no
    -o UserKnownHostsFile=/dev/null
    -o BatchMode=yes
    -o ConnectTimeout=50
    -o ServerAliveInterval=25
    -o ServerAliveCountMax=15
    -i "$SSH_USER_KEY"
    -p "${CLICKHOUSE_PORT_SSH}"
)

# Create a user with SSH key authentication
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${SSH_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${SSH_USER} IDENTIFIED WITH ssh_key BY KEY '${SSH_USER_PUBKEY}' TYPE 'ssh-ed25519'"
${CLICKHOUSE_CLIENT} --query "GRANT ALL ON ${CLICKHOUSE_DATABASE}.* TO ${SSH_USER}"

# Run queries via SSH, capturing stderr for diagnostics on failure
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${CLICKHOUSE_HOST}" "SELECT 1" 2>"$SSH_STDERR" || cat "$SSH_STDERR" >&2

ssh "${SSH_OPTS[@]}" "${SSH_USER}@${CLICKHOUSE_HOST}" "SELECT currentUser() = '${SSH_USER}'" 2>"$SSH_STDERR" || cat "$SSH_STDERR" >&2

# Clean up
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${SSH_USER}"
rm -f "$SSH_USER_KEY" "$SSH_STDERR"
