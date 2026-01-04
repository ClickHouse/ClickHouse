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

echo "=== Testing Multiple double SHA1 Passwords ==="
echo "Testing first double SHA1 password:"
echo "SELECT 'double_sha1_success_1'" | ${CLICKHOUSE_CLIENT} --user=multi_double_sha1_user --password=double_sha1_pass_1 2>/dev/null && echo "SUCCESS" || echo "FAILED"
echo "Testing second double SHA1 password:"
echo "SELECT 'double_sha1_success_2'" | ${CLICKHOUSE_CLIENT} --user=multi_double_sha1_user --password=double_sha1_pass_2 2>/dev/null && echo "SUCCESS" || echo "FAILED"
echo "Testing wrong double SHA1 password:"
echo "SELECT 'should_fail'" | ${CLICKHOUSE_CLIENT} --user=multi_double_sha1_user --password=wrong_double_sha1_pass 2>/dev/null && echo "UNEXPECTED_SUCCESS" || echo "EXPECTED_FAILURE"

echo "=== Testing Mixed Authentication Methods ==="
echo "Testing mixed auth plaintext password 1:"
echo "SELECT 'mixed_plain_success_1'" | ${CLICKHOUSE_CLIENT} --user=mixed_auth_user --password=mixed_plain_pass_1 2>/dev/null && echo "SUCCESS" || echo "FAILED"
echo "Testing mixed auth plaintext password 2:"
echo "SELECT 'mixed_plain_success_2'" | ${CLICKHOUSE_CLIENT} --user=mixed_auth_user --password=mixed_plain_pass_2 2>/dev/null && echo "SUCCESS" || echo "FAILED"
echo "Testing mixed auth double SHA1 password 1:"
echo "SELECT 'mixed_double_sha1_success_1'" | ${CLICKHOUSE_CLIENT} --user=mixed_auth_user --password=mixed_double_sha1_pass_1 2>/dev/null && echo "SUCCESS" || echo "FAILED"
echo "Testing mixed auth double SHA1 password 2:"
echo "SELECT 'mixed_double_sha1_success_2'" | ${CLICKHOUSE_CLIENT} --user=mixed_auth_user --password=mixed_double_sha1_pass_2 2>/dev/null && echo "SUCCESS" || echo "FAILED"
