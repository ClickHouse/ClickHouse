#!/usr/bin/env bash

# Test that `mergeTreeIndex` enforces the source table's row policy. This introspection exposes
# granule-level primary key values (and, with marks, per-column mark offsets) for every row, including
# rows a row policy is supposed to hide. So ANY effective (present and not always-true) row policy on
# the source table must block the read, regardless of which columns the policy predicate references.
# Only the parent-column SELECT grant is given, to also cover the subcolumn-to-parent access-check
# mapping for the primary key `t.a`.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_test_user_03812_rp"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab;
DROP USER IF EXISTS $user_name;

CREATE TABLE tab
(
    t Tuple(a UInt64, secret UInt64),
    v UInt64
)
ENGINE = MergeTree
ORDER BY t.a
SETTINGS index_granularity = 3, min_bytes_for_wide_part = 0;

INSERT INTO tab SELECT tuple(number, number + 100), number FROM numbers(12);

CREATE USER $user_name;
-- Only a parent-column grant: reading the subcolumn primary key t.a is authorized via t.
GRANT SELECT(t) ON $CLICKHOUSE_DATABASE.tab TO $user_name;
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
    $CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p_03812 ON $CLICKHOUSE_DATABASE.tab FOR SELECT USING $1 TO $user_name;"
    check_access "$2"
    $CLICKHOUSE_CLIENT -q "DROP ROW POLICY p_03812 ON $CLICKHOUSE_DATABASE.tab;"
}

query="SELECT t.a FROM mergeTreeIndex(currentDatabase(), tab)"

# Baseline: parent-column grant, no row policy -> allowed.
check_access "$query"

# Any effective row policy blocks the read, whatever column its predicate references:
#  - an unrelated column (`v`) that does not overlap the exposed index columns,
check_with_policy "v < 5" "$query"
#  - the whole parent column `t`,
check_with_policy "toString(t) != ''" "$query"
#  - the exact primary key subcolumn `t.a`,
check_with_policy "t.a < 5" "$query"
#  - a sibling subcolumn `t.secret` of the same tuple column,
check_with_policy "t.secret < 105" "$query"
#  - an always-false filter that references no column at all (hides every row).
check_with_policy "0" "$query"

# An always-true policy hides nothing, so it must NOT block the read.
check_with_policy "1" "$query"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS tab;
    DROP USER IF EXISTS $user_name;
"
