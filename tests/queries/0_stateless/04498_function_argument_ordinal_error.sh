#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: Depends on OpenSSL

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query="SELECT encrypt('aes-256-gcm', 'plaintext', '12345678901234567890123456789012', 12345)"
output=$(${CLICKHOUSE_LOCAL} --query="$query" 2>&1)

echo "$output" | grep -F -q "4th argument 'IV'" && echo "OK" || echo "FAIL"
echo "$output" | grep -F -q "3th argument 'IV'" && echo "FAIL" || echo "OK"
