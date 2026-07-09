#!/usr/bin/env bash

# Test that `mergeTreeAnalyzeIndexes` enforces the source table's row policy. This introspection exposes
# which primary-key and skip-index mark ranges survive a predicate for each part, revealing granule-level
# structure for rows a row policy is supposed to hide. So ANY effective (present and not always-true) row
# policy on the source table must block the read, regardless of which columns the policy references.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_test_user_04509"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab;
DROP USER IF EXISTS $user_name;

CREATE TABLE tab
(
    key UInt64,
    v UInt64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS index_granularity = 3, min_bytes_for_wide_part = 0;

INSERT INTO tab SELECT number, number + 100 FROM numbers(12);

CREATE USER $user_name;
GRANT SELECT ON $CLICKHOUSE_DATABASE.tab TO $user_name;
"

function check_access()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$user_name" -q "$1" 2>&1)
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
    $CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p_04509 ON $CLICKHOUSE_DATABASE.tab FOR SELECT USING $1 TO $user_name;"
    check_access "$2"
    $CLICKHOUSE_CLIENT -q "DROP ROW POLICY p_04509 ON $CLICKHOUSE_DATABASE.tab;"
}

query="SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), tab, key >= 5)"

# Baseline: SELECT grant, no row policy -> allowed.
check_access "$query"

# Any effective row policy blocks the read, whatever column its predicate references:
#  - the primary key column `key`,
check_with_policy "key < 5" "$query"
#  - an unrelated column (`v`) that is not part of any index,
check_with_policy "v < 5" "$query"
#  - an always-false filter that references no column at all (hides every row).
check_with_policy "0" "$query"

# An always-true policy hides nothing, so it must NOT block the read.
check_with_policy "1" "$query"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS tab;
    DROP USER IF EXISTS $user_name;
"
