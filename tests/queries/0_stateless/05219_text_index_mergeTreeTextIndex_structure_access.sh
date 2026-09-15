#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The structure of `mergeTreeTextIndex` depends on the tokenizer of the source index, so resolving it
# (e.g. `DESCRIBE`) requires `SHOW TABLES` on the source table: the grant that also reveals the index
# definition in `system.data_skipping_indices`. Reading the tokens still requires `SELECT` on the indexed columns.

user_name="${CLICKHOUSE_DATABASE}_test_user_05219"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab;
DROP USER IF EXISTS $user_name;

CREATE TABLE tab
(
    id UInt32,
    m Map(String, String),
    INDEX idx_kv m TYPE text(tokenizer = 'keyValuePairs'),
    INDEX idx_minmax id TYPE minmax
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, {'level':'error'});

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
"

function check_access()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "$1" 2>&1)
    local rc=$?
    local code
    code=$(echo "$output" | grep -oE '\([A-Z_]+\)' | tail -1)
    if [ $rc -eq 0 ]; then
        echo "OK"
    elif [ -n "$code" ]; then
        echo "$code"
    else
        echo "$output"
    fi
}

echo "-- no grants: nothing about the table or its indexes is revealed"
check_access "DESCRIBE mergeTreeTextIndex(currentDatabase(), tab, idx_kv)"
check_access "DESCRIBE mergeTreeTextIndex(currentDatabase(), tab, idx_minmax)"
check_access "DESCRIBE mergeTreeTextIndex(currentDatabase(), tab, idx_missing)"
check_access "DESCRIBE mergeTreeTextIndex(currentDatabase(), tab_missing, idx_kv)"
check_access "SELECT token_key FROM mergeTreeTextIndex(currentDatabase(), tab, idx_kv)"

$CLICKHOUSE_CLIENT -q "GRANT SHOW TABLES ON $CLICKHOUSE_DATABASE.tab TO $user_name"

echo "-- SHOW TABLES on the table: the structure resolves, reading the tokens is still denied"
check_access "DESCRIBE mergeTreeTextIndex(currentDatabase(), tab, idx_kv)"
check_access "DESCRIBE mergeTreeTextIndex(currentDatabase(), tab, idx_minmax)"
check_access "DESCRIBE mergeTreeTextIndex(currentDatabase(), tab, idx_missing)"
check_access "DESCRIBE mergeTreeTextIndex(currentDatabase(), tab_missing, idx_kv)"
check_access "SELECT token_key FROM mergeTreeTextIndex(currentDatabase(), tab, idx_kv)"

$CLICKHOUSE_CLIENT -q "REVOKE SHOW TABLES ON $CLICKHOUSE_DATABASE.tab FROM $user_name"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(m) ON $CLICKHOUSE_DATABASE.tab TO $user_name"

echo "-- SELECT on the indexed column implies SHOW TABLES: both work"
check_access "DESCRIBE mergeTreeTextIndex(currentDatabase(), tab, idx_kv)"
check_access "SELECT token_key, token_value FROM mergeTreeTextIndex(currentDatabase(), tab, idx_kv)"

$CLICKHOUSE_CLIENT -q "
DROP TABLE tab;
DROP USER $user_name;
"
