#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_test_user_03917"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab;
DROP USER IF EXISTS $user_name;

CREATE TABLE tab
(
    a String,
    b String,
    INDEX idx_a a TYPE text (tokenizer = 'splitByNonAlpha'),
    INDEX idx_b b TYPE text (tokenizer = 'splitByNonAlpha'),
    INDEX idx_ab concat(a, b) TYPE text (tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    index_granularity = 3,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 6,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    compact_parts_max_granules_to_buffer = 1;

INSERT INTO tab (a, b) VALUES ('hello', 'world');

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
REVOKE SELECT ON $CLICKHOUSE_DATABASE.tab FROM $user_name;
"

function check_access()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "$1" 2>&1)
    local rc=$?
    if [ $rc -eq 0 ]; then
        echo "OK"
    elif echo "$output" | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    else
        echo "$output"
    fi
}

check_access "SELECT a FROM tab"
check_access "SELECT b FROM tab"

check_access "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_a)"
check_access "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_b)"
check_access "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_ab)"

$CLICKHOUSE_CLIENT -q "GRANT SELECT (b) ON $CLICKHOUSE_DATABASE.tab TO $user_name;"

check_access "SELECT a FROM tab"
check_access "SELECT b FROM tab"

check_access "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_a)"
check_access "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_b)"
check_access "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_ab)"

## Row policy tests
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $CLICKHOUSE_DATABASE.tab TO $user_name;"

# Row policy on column `a`: idx_a and idx_ab denied, idx_b allowed
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p1_03917 ON $CLICKHOUSE_DATABASE.tab FOR SELECT USING a = 'hello' TO $user_name;"

check_access "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_a)"
check_access "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_b)"
check_access "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_ab)"

$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p1_03917 ON $CLICKHOUSE_DATABASE.tab;"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS tab;
    DROP USER IF EXISTS $user_name;
"
