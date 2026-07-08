#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_test_user_04504"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab;
DROP USER IF EXISTS $user_name;

CREATE TABLE tab
(
    json JSON,
    other String,
    INDEX idx_ab (json.a.b::String) TYPE text (tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    index_granularity = 3,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 6,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    compact_parts_max_granules_to_buffer = 1;

INSERT INTO tab VALUES ('{\"a\": {\"b\": \"hello\"}, \"flag\": \"secret\"}', 'row1');

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
GRANT SELECT ON $CLICKHOUSE_DATABASE.tab TO $user_name;
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

# Baseline: full SELECT grant and no row policy -> allowed.
check_access "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_ab)"

# Regression: a row policy that references the whole parent column `json`, while the index is
# built on the subcolumn `json.a.b`. Both map to the storage column `json`, so reading the index
# would expose tokens for rows the policy should hide and must be denied.
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p0_04504 ON $CLICKHOUSE_DATABASE.tab FOR SELECT USING toString(json) != '' TO $user_name;"
check_access "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_ab)"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p0_04504 ON $CLICKHOUSE_DATABASE.tab;"

# Regression: a row policy on a sibling subcolumn (`json.flag`) of the same JSON column.
# The index requires `json.a.b`; both `json.flag` and `json.a.b` map to the parent storage
# column `json`, so reading the index would expose tokens for rows the policy should hide.
# Before the fix the raw names differed (`json.flag` != `json.a.b`) and the policy was bypassed.
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p1_04504 ON $CLICKHOUSE_DATABASE.tab FOR SELECT USING json.flag::String = 'keep' TO $user_name;"
check_access "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_ab)"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p1_04504 ON $CLICKHOUSE_DATABASE.tab;"

# A row policy on the exact index subcolumn (`json.a.b`) is also denied.
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p2_04504 ON $CLICKHOUSE_DATABASE.tab FOR SELECT USING json.a.b::String = 'hello' TO $user_name;"
check_access "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_ab)"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p2_04504 ON $CLICKHOUSE_DATABASE.tab;"

# A row policy on an unrelated column (`other`) does not touch the index columns -> allowed.
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p3_04504 ON $CLICKHOUSE_DATABASE.tab FOR SELECT USING other = 'row1' TO $user_name;"
check_access "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_ab)"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p3_04504 ON $CLICKHOUSE_DATABASE.tab;"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS tab;
    DROP USER IF EXISTS $user_name;
"
