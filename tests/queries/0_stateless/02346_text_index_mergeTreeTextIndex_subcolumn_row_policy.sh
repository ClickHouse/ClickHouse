#!/usr/bin/env bash

# Test that `mergeTreeTextIndex` enforces the source table's row policy. The text index is derived from
# the source columns of every row, including rows a row policy is supposed to hide, so reading its tokens
# could leak their contents. So ANY effective (present and not always-true) row policy on the source
# table must block the read, regardless of which columns the policy predicate references.

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

function check_with_policy()
{
    # $1 = row policy USING expression, $2 = query to run as the restricted user.
    $CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p_04504 ON $CLICKHOUSE_DATABASE.tab FOR SELECT USING $1 TO $user_name;"
    check_access "$2"
    $CLICKHOUSE_CLIENT -q "DROP ROW POLICY p_04504 ON $CLICKHOUSE_DATABASE.tab;"
}

query="SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_ab)"

# Baseline: full SELECT grant and no row policy -> allowed.
check_access "$query"

# Any effective row policy blocks the read, whatever column its predicate references:
#  - an unrelated column (`other`) that does not overlap the index columns,
check_with_policy "other = 'row1'" "$query"
#  - the whole parent column `json` (the index is built on the subcolumn `json.a.b`),
check_with_policy "toString(json) != ''" "$query"
#  - the exact index subcolumn `json.a.b`,
check_with_policy "json.a.b::String = 'hello'" "$query"
#  - a sibling subcolumn `json.flag` of the same JSON column,
check_with_policy "json.flag::String = 'keep'" "$query"
#  - an always-false filter that references no column at all (hides every row).
check_with_policy "0" "$query"

# An always-true policy hides nothing, so it must NOT block the read.
check_with_policy "1" "$query"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS tab;
    DROP USER IF EXISTS $user_name;
"
