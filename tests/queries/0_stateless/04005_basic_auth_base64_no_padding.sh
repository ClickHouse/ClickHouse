#!/usr/bin/env bash
# Tags: no-fasttest

# Test that HTTP basic auth accepts base64-encoded credentials
# without padding ('=' characters). Some HTTP clients omit padding
# in the Authorization header.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# "default:" base64-padded is "ZGVmYXVsdDo=", unpadded is "ZGVmYXVsdDo"

# Padded — should work
${CLICKHOUSE_CURL} -sS -H "Authorization: Basic ZGVmYXVsdDo=" "${CLICKHOUSE_URL}&query=SELECT+1"

# Unpadded — should also work (this is the bug fix)
${CLICKHOUSE_CURL} -sS -H "Authorization: Basic ZGVmYXVsdDo" "${CLICKHOUSE_URL}&query=SELECT+1"

# Test with 2 padding chars: create a temporary user whose credentials
# produce base64 with "==" padding.
# "test_b64:pwd" → base64 = "dGVzdF9iNjQ6cHdk" (no padding needed, length divisible by 3)
# "test_b64:pw"  → base64 = "dGVzdF9iNjQ6cHc=" (1 padding char)
# "test_b64:p"   → base64 = "dGVzdF9iNjQ6cA==" (2 padding chars)

${CLICKHOUSE_CLIENT} -q "CREATE USER IF NOT EXISTS test_b64 IDENTIFIED BY 'p'"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON system.one TO test_b64"

# Padded (2x '=')
${CLICKHOUSE_CURL} -sS -H "Authorization: Basic dGVzdF9iNjQ6cA==" "${CLICKHOUSE_URL}&query=SELECT+1"

# Unpadded
${CLICKHOUSE_CURL} -sS -H "Authorization: Basic dGVzdF9iNjQ6cA" "${CLICKHOUSE_URL}&query=SELECT+1"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS test_b64"
