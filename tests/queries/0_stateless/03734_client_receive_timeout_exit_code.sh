#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set +e

# Test that receive_timeout returns exit code 159 (TIMEOUT_EXCEEDED)
# Set interactive_delay to a very large value to prevent progress packets from resetting the timeout
$CLICKHOUSE_CLIENT --receive_timeout=1 --interactive_delay=10000000 --query="SELECT sleep(2) FORMAT Null" >/dev/null 2>&1
echo $?

# Verify the timeout error message is correctly printed (capture stderr)
$CLICKHOUSE_CLIENT --receive_timeout=1 --interactive_delay=10000000 --query="SELECT sleep(2) FORMAT Null" 2>&1 | grep -q "Timeout exceeded"
echo $?
