#!/usr/bin/env bash

# Test that `mergeTreeIndex` enforces the source table's row policy, including when the
# primary key is a subcolumn (e.g. `t.a`) that is normalized to its parent storage column
# (`t`) for the access check. A row policy on the parent column or on a sibling subcolumn
# must block the read; otherwise a user with only `GRANT SELECT(t)` could read granule-level
# primary key values for rows the policy is supposed to hide.

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

# Baseline: parent-column grant, no row policy -> allowed.
check_access "SELECT t.a FROM mergeTreeIndex(currentDatabase(), tab)"

# Regression: a row policy on a sibling subcolumn (`t.secret`) of the same tuple column.
# The primary key requires `t.a`; both `t.secret` and `t.a` map to the parent storage column
# `t`, so reading granule key values would leak rows the policy should hide. Before the fix
# the raw names differed (`t.secret` != `t.a`) and the policy was silently bypassed.
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p1_03812 ON $CLICKHOUSE_DATABASE.tab FOR SELECT USING t.secret < 105 TO $user_name;"
check_access "SELECT t.a FROM mergeTreeIndex(currentDatabase(), tab)"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p1_03812 ON $CLICKHOUSE_DATABASE.tab;"

# A row policy that references the whole parent column `t` is also denied.
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p2_03812 ON $CLICKHOUSE_DATABASE.tab FOR SELECT USING toString(t) != '' TO $user_name;"
check_access "SELECT t.a FROM mergeTreeIndex(currentDatabase(), tab)"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p2_03812 ON $CLICKHOUSE_DATABASE.tab;"

# A row policy on the exact primary key subcolumn (`t.a`) is denied.
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p3_03812 ON $CLICKHOUSE_DATABASE.tab FOR SELECT USING t.a < 5 TO $user_name;"
check_access "SELECT t.a FROM mergeTreeIndex(currentDatabase(), tab)"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p3_03812 ON $CLICKHOUSE_DATABASE.tab;"

# A row policy on an unrelated column (`v`) does not touch the index columns -> allowed.
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p4_03812 ON $CLICKHOUSE_DATABASE.tab FOR SELECT USING v < 5 TO $user_name;"
check_access "SELECT t.a FROM mergeTreeIndex(currentDatabase(), tab)"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p4_03812 ON $CLICKHOUSE_DATABASE.tab;"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS tab;
    DROP USER IF EXISTS $user_name;
"
