#!/usr/bin/env bash
# Tags: no-fasttest
# encrypt functions don't exist in the fasttest build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TEST_ENCRYPT_FUNC="test_encrypt_func_${CLICKHOUSE_DATABASE}"
TEST_DECRYPT_FUNC="test_decrypt_func_${CLICKHOUSE_DATABASE}"
KEY="TOPSECRET-TOPSECRET-TOPSECRET---"

$CLICKHOUSE_CLIENT -q "CREATE FUNCTION ${TEST_ENCRYPT_FUNC} AS (plaintext) -> encrypt('aes-256-ofb', plaintext, '${KEY}')"
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION ${TEST_DECRYPT_FUNC} AS (ciphertext) -> decrypt('aes-256-ofb', ciphertext, '${KEY}')"

$CLICKHOUSE_CLIENT -q "SELECT 'plaintext' AS plaintext, hex(${TEST_ENCRYPT_FUNC}(plaintext) AS ciphertext), ${TEST_DECRYPT_FUNC}(ciphertext)"

$CLICKHOUSE_CLIENT -q "SELECT name, create_query FROM system.functions WHERE name IN ['${TEST_ENCRYPT_FUNC}', '${TEST_DECRYPT_FUNC}'] ORDER BY name"

$CLICKHOUSE_CLIENT -q "DROP FUNCTION ${TEST_ENCRYPT_FUNC}"
$CLICKHOUSE_CLIENT -q "DROP FUNCTION ${TEST_DECRYPT_FUNC}"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE (query LIKE '%TOPSECRET%') AND (current_database = currentDatabase())"
