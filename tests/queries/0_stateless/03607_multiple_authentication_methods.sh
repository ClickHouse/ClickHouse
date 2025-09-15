#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "=== Testing Multiple Plaintext Passwords ==="
echo "Testing first password:"
echo "SELECT 'auth_success_1'" | ${CLICKHOUSE_CLIENT} --user=multi_plaintext_user --password=plain_pass_1 2>/dev/null && echo "SUCCESS" || echo "FAILED"
echo "Testing second password:"
echo "SELECT 'auth_success_2'" | ${CLICKHOUSE_CLIENT} --user=multi_plaintext_user --password=plain_pass_2 2>/dev/null && echo "SUCCESS" || echo "FAILED"
echo "Testing wrong password:"
echo "SELECT 'should_fail'" | ${CLICKHOUSE_CLIENT} --user=multi_plaintext_user --password=bad_pass 2>/dev/null && echo "UNEXPECTED_SUCCESS" || echo "EXPECTED_FAILURE"

echo "=== Testing Multiple SHA256 Passwords ==="
echo "Testing first SHA256 password:"
echo "SELECT 'sha256_success_1'" | ${CLICKHOUSE_CLIENT} --user=multi_sha256_user --password=sha_pass_1 2>/dev/null && echo "SUCCESS" || echo "FAILED"
echo "Testing second SHA256 password:"
echo "SELECT 'sha256_success_2'" | ${CLICKHOUSE_CLIENT} --user=multi_sha256_user --password=sha_pass_2 2>/dev/null && echo "SUCCESS" || echo "FAILED"
echo "Testing wrong SHA256 password:"
echo "SELECT 'should_fail'" | ${CLICKHOUSE_CLIENT} --user=multi_sha256_user --password=_sha_pass 2>/dev/null && echo "UNEXPECTED_SUCCESS" || echo "EXPECTED_FAILURE"

echo "=== Testing Mixed Authentication Methods ==="
echo "Testing mixed auth plaintext password:"
echo "SELECT 'mixed_plain_success'" | ${CLICKHOUSE_CLIENT} --user=mixed_auth_user --password=mixed_plain_pass 2>/dev/null && echo "SUCCESS" || echo "FAILED"
echo "Testing mixed auth SHA256 password:"
echo "SELECT 'mixed_sha_success'" | ${CLICKHOUSE_CLIENT} --user=mixed_auth_user --password=mixed_sha_pass 2>/dev/null && echo "SUCCESS" || echo "FAILED"

echo "=== Multiple Authentication Methods Test Complete ==="
