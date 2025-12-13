#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test multiple authentication methods feature introduced in commit e84bb91c6f1

# Create temporary users config file with multiple authentication methods
users_config_file="${CLICKHOUSE_TMP}/users_multiple_auth_${CLICKHOUSE_TEST_UNIQUE_NAME}.xml"

cat > "$users_config_file" << 'EOF'
<?xml version="1.0"?>
<clickhouse>
    <users>
        <!-- User with multiple plaintext passwords -->
        <multi_plaintext_user>
            <password>
                <pass1>test_pass_1</pass1>
                <pass2>test_pass_2</pass2>
                <pass3>test_pass_3</pass3>
            </password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </multi_plaintext_user>

        <!-- User with multiple SHA256 passwords -->
        <multi_sha256_user>
            <password_sha256_hex>
                <!-- Hash of "password1": ef92b778bafe771e89245b89ecbc08a44a4e166c06659911881f383d4473e94f -->
                <hash1>ef92b778bafe771e89245b89ecbc08a44a4e166c06659911881f383d4473e94f</hash1>
                <!-- Hash of "password2": 6cf615d5bcaac778352a8f1f3360d23f02f34ec182e259897fd6ce485d7870d4 -->
                <hash2>6cf615d5bcaac778352a8f1f3360d23f02f34ec182e259897fd6ce485d7870d4</hash2>
            </password_sha256_hex>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </multi_sha256_user>

        <!-- User with mixed authentication methods -->
        <mixed_auth_user>
            <password>mixed_plain_pass</password>
            <password_sha256_hex>
                <!-- Hash of "mixed_sha_pass": 8d23cf6c86e834a7aa6eded54c26ce2bb2e74903538c61bdd5d2197997ab2f72 -->
                <hash1>8d23cf6c86e834a7aa6eded54c26ce2bb2e74903538c61bdd5d2197997ab2f72</hash1>
            </password_sha256_hex>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </mixed_auth_user>

        <!-- User with single password (backward compatibility) -->
        <single_password_user>
            <password>single_test_pass</password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </single_password_user>
    </users>
</clickhouse>
EOF

# Start ClickHouse server with the custom users config
${CLICKHOUSE_CLIENT} --query "SYSTEM RELOAD CONFIG" 2>/dev/null || true

echo "=== Testing Multiple Plaintext Passwords ==="

# Test authentication with first password
echo "Testing first password:"
echo "SELECT 'auth_success_1'" | ${CLICKHOUSE_CLIENT} --user=multi_plaintext_user --password=test_pass_1 2>/dev/null && echo "SUCCESS" || echo "FAILED"

# Test authentication with second password
echo "Testing second password:"
echo "SELECT 'auth_success_2'" | ${CLICKHOUSE_CLIENT} --user=multi_plaintext_user --password=test_pass_2 2>/dev/null && echo "SUCCESS" || echo "FAILED"

# Test authentication with third password
echo "Testing third password:"
echo "SELECT 'auth_success_3'" | ${CLICKHOUSE_CLIENT} --user=multi_plaintext_user --password=test_pass_3 2>/dev/null && echo "SUCCESS" || echo "FAILED"

# Test authentication with wrong password (should fail)
echo "Testing wrong password:"
echo "SELECT 'should_fail'" | ${CLICKHOUSE_CLIENT} --user=multi_plaintext_user --password=wrong_password 2>/dev/null && echo "UNEXPECTED_SUCCESS" || echo "EXPECTED_FAILURE"

echo "=== Testing Multiple SHA256 Passwords ==="

# Test authentication with first SHA256 password (password1)
echo "Testing first SHA256 password:"
echo "SELECT 'sha256_success_1'" | ${CLICKHOUSE_CLIENT} --user=multi_sha256_user --password=password1 2>/dev/null && echo "SUCCESS" || echo "FAILED"

# Test authentication with second SHA256 password (password2)
echo "Testing second SHA256 password:"
echo "SELECT 'sha256_success_2'" | ${CLICKHOUSE_CLIENT} --user=multi_sha256_user --password=password2 2>/dev/null && echo "SUCCESS" || echo "FAILED"

# Test authentication with wrong SHA256 password (should fail)
echo "Testing wrong SHA256 password:"
echo "SELECT 'should_fail'" | ${CLICKHOUSE_CLIENT} --user=multi_sha256_user --password=wrong_sha_password 2>/dev/null && echo "UNEXPECTED_SUCCESS" || echo "EXPECTED_FAILURE"

echo "=== Testing Mixed Authentication Methods ==="

# Test authentication with plaintext password
echo "Testing mixed auth plaintext password:"
echo "SELECT 'mixed_plain_success'" | ${CLICKHOUSE_CLIENT} --user=mixed_auth_user --password=mixed_plain_pass 2>/dev/null && echo "SUCCESS" || echo "FAILED"

# Test authentication with SHA256 password (mixed_sha_pass)
echo "Testing mixed auth SHA256 password:"
echo "SELECT 'mixed_sha_success'" | ${CLICKHOUSE_CLIENT} --user=mixed_auth_user --password=mixed_sha_pass 2>/dev/null && echo "SUCCESS" || echo "FAILED"

echo "=== Testing Backward Compatibility ==="

# Test single password user (backward compatibility)
echo "Testing single password user:"
echo "SELECT 'single_success'" | ${CLICKHOUSE_CLIENT} --user=single_password_user --password=single_test_pass 2>/dev/null && echo "SUCCESS" || echo "FAILED"

echo "=== Testing System Tables ==="

# Check that users appear in system.users table
echo "Users in system.users:"
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.users WHERE name LIKE '%_user' ORDER BY name" 2>/dev/null || echo "SYSTEM_TABLE_ERROR"

echo "=== Testing User Management ==="

# Test creating and dropping users via SQL
${CLICKHOUSE_CLIENT} --query "CREATE USER IF NOT EXISTS test_sql_user IDENTIFIED BY 'sql_test_pass'" 2>/dev/null || echo "CREATE_USER_ERROR"

# Test that the SQL-created user works
echo "Testing SQL-created user:"
echo "SELECT 'sql_user_success'" | ${CLICKHOUSE_CLIENT} --user=test_sql_user --password=sql_test_pass 2>/dev/null && echo "SUCCESS" || echo "FAILED"

# Clean up SQL-created user
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS test_sql_user" 2>/dev/null || echo "DROP_USER_ERROR"

echo "=== Testing Configuration Validation ==="

# Test that the configuration is loaded correctly by checking user existence
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.users WHERE name IN ('multi_plaintext_user', 'multi_sha256_user', 'mixed_auth_user', 'single_password_user')" 2>/dev/null || echo "CONFIG_VALIDATION_ERROR"

# Clean up temporary config file
rm -f "$users_config_file"

echo "=== Multiple Authentication Methods Test Complete ==="
