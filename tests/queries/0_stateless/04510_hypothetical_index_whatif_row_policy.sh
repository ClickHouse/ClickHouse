#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: hypothetical indexes are session-scoped

# Test that `EXPLAIN WHATIF` index estimation enforces the source table's row policy. The estimate
# (skip ratio) is derived from every row, including rows a row policy is supposed to hide, so a user
# could infer the distribution of hidden rows from it. A column-level SELECT grant does not cover this,
# so ANY effective (present and not always-true) row policy must block the estimate, regardless of which
# columns the policy predicate references.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_test_user_04510"

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

# A hypothetical index is session-scoped, so create it and run EXPLAIN WHATIF in the same client session.
function check_with_policy()
{
    # $1 = row policy USING expression, or empty for no policy.
    if [ -n "$1" ]; then
        $CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p_04510 ON $CLICKHOUSE_DATABASE.tab FOR SELECT USING $1 TO $user_name;"
    fi

    local output
    output=$($CLICKHOUSE_CLIENT --user "$user_name" -n -q "
        CREATE HYPOTHETICAL INDEX idx_v ON tab (v) TYPE minmax GRANULARITY 1;
        EXPLAIN WHATIF SELECT key FROM tab WHERE v = 105;
    " 2>&1)
    local rc=$?
    if [ $rc -eq 0 ]; then
        echo "OK"
    elif echo "$output" | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    else
        echo "$output"
    fi

    if [ -n "$1" ]; then
        $CLICKHOUSE_CLIENT -q "DROP ROW POLICY p_04510 ON $CLICKHOUSE_DATABASE.tab;"
    fi
}

# Baseline: SELECT grant, no row policy -> allowed.
check_with_policy ""

# Any effective row policy blocks the estimate, whatever column its predicate references:
#  - the index column `v`,
check_with_policy "v < 5"
#  - the primary key column `key`,
check_with_policy "key < 5"
#  - an always-false filter that references no column at all (hides every row).
check_with_policy "0"

# An always-true policy hides nothing, so it must NOT block the estimate.
check_with_policy "1"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS tab;
    DROP USER IF EXISTS $user_name;
"
