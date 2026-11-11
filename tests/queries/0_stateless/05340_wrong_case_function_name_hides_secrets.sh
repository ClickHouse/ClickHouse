#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no S3 or MySQL table functions, whose wrong-case names then stay visible

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A call that differs only in case from a table function or function with secrets is rejected after the statement is
# logged, so every argument is hidden. Other unknown or case-insensitive functions stay visible.
while read -r query; do
    $CLICKHOUSE_FORMAT --oneline --query "$query"
done <<'EOF'
SELECT * FROM S3('http://bucket.s3.amazonaws.com/file.csv', 'access_key_id', 'plain_secret_key', 'CSV')
SELECT * FROM MySQL('host:3306', 'db', 'table', 'user', 'plain_password')
SELECT * FROM URL('https://user:plain_password@example.com/data', 'CSV')
SELECT * FROM REMOTE('host', 'db', 'table', 'user', 'plain_password')
INSERT INTO TABLE FUNCTION S3('http://bucket.s3.amazonaws.com/file.csv', 'access_key_id', 'plain_secret_key', 'CSV', 'x UInt64') SELECT 1
SELECT ENCRYPT('aes-256-ofb', 'plain_text', 'plain_key')
SELECT * FROM s3('http://bucket.s3.amazonaws.com/file.csv', 'access_key_id', 'plain_secret_key', 'CSV')
SELECT * FROM NUMBERS(1)
SELECT noSuchFunction('visible')
SELECT UPPER('visible')
EOF

$CLICKHOUSE_CLIENT --log_queries=1 --query "SELECT * FROM S3('http://bucket.s3.amazonaws.com/file.csv', 'access_key_id', 'plain_secret_e2e', 'CSV')" 2>&1 | grep -o -m1 UNKNOWN_FUNCTION
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT query FROM system.query_log WHERE current_database = currentDatabase() AND type = 'ExceptionBeforeStart' AND exception_code = 46 AND event_date >= yesterday()"
