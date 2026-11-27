#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_USER="user_03732_$CLICKHOUSE_DATABASE"
CLICKHOUSE_USER_CLIENT="$CLICKHOUSE_CLIENT --user $CLICKHOUSE_USER --password user_password"

$CLICKHOUSE_CLIENT --multiline -q """
    CREATE TABLE test_table_1 (key UInt32) Engine=MergeTree() ORDER BY key;
    CREATE TABLE test_table_2 (key UInt64) Engine=MergeTree() ORDER BY key;
    CREATE TABLE test_table_3 (key Int32) Engine=MergeTree() ORDER BY key;
    CREATE TABLE test_merge_ro Engine=Merge($CLICKHOUSE_DATABASE, 'test_table_\d+');
    CREATE TABLE test_merge_explicit Engine=Merge($CLICKHOUSE_DATABASE, 'test_table_\d+', 'test_table_1');
    CREATE TABLE test_merge_auto (key Int64) Engine=Merge($CLICKHOUSE_DATABASE, 'test_table_\d+', auto);
    DROP USER IF EXISTS $CLICKHOUSE_USER;
    CREATE USER $CLICKHOUSE_USER IDENTIFIED WITH plaintext_password BY 'user_password';
    GRANT INSERT ON $CLICKHOUSE_DATABASE.test_merge_ro TO $CLICKHOUSE_USER;
"""

$CLICKHOUSE_USER_CLIENT --multiline -q """
    INSERT INTO test_merge_ro VALUES (1); -- { serverError TABLE_IS_READ_ONLY }
    INSERT INTO test_merge_explicit VALUES (2); -- { serverError ACCESS_DENIED }
    INSERT INTO test_merge_auto VALUES (3); -- { serverError ACCESS_DENIED }
"""

# No access to inner table
$CLICKHOUSE_CLIENT --multiline -q """
    GRANT INSERT ON $CLICKHOUSE_DATABASE.test_merge_explicit TO $CLICKHOUSE_USER;
    GRANT INSERT ON $CLICKHOUSE_DATABASE.test_merge_auto TO $CLICKHOUSE_USER;
"""
$CLICKHOUSE_USER_CLIENT --multiline -q """
    INSERT INTO test_merge_explicit VALUES (2); -- { serverError ACCESS_DENIED }
    INSERT INTO test_merge_auto VALUES (2); -- { serverError UNKNOWN_TABLE }
"""

# Has access to both tables
$CLICKHOUSE_CLIENT -q "GRANT INSERT,SELECT ON $CLICKHOUSE_DATABASE.test_table_1 TO $CLICKHOUSE_USER;"
$CLICKHOUSE_USER_CLIENT --multiline -q """
    SELECT '-- insert in the explicit merge table';
    INSERT INTO test_merge_explicit VALUES (1);
    INSERT INTO test_merge_auto VALUES (2);
    SELECT * FROM test_table_1 ORDER BY ALL;
"""

# Only SHOW access to the auto write table
$CLICKHOUSE_CLIENT -q "GRANT SHOW ON $CLICKHOUSE_DATABASE.test_table_2 TO $CLICKHOUSE_USER;"
$CLICKHOUSE_USER_CLIENT -q "INSERT INTO test_merge_auto VALUES (3); -- { serverError ACCESS_DENIED }"

# Only SHOW access to the auto write table
$CLICKHOUSE_CLIENT -q "GRANT INSERT,SELECT ON $CLICKHOUSE_DATABASE.test_table_3 TO $CLICKHOUSE_USER;"
$CLICKHOUSE_USER_CLIENT --multiline -q """
    INSERT INTO test_merge_auto VALUES (3);
    SELECT '-- insert in the latest auto table';
    SELECT * FROM test_table_3 ORDER BY ALL;
"""

CLICKHOUSE_VIEW_USER="view_user_03732_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --multiline -q """
    CREATE TABLE inner_view (key UInt64) Engine=MergeTree() ORDER BY key;
    DROP USER IF EXISTS $CLICKHOUSE_VIEW_USER;
    CREATE USER $CLICKHOUSE_VIEW_USER IDENTIFIED WITH plaintext_password BY 'user_password';
    GRANT SELECT ON $CLICKHOUSE_DATABASE.test_merge_explicit TO $CLICKHOUSE_VIEW_USER;
    GRANT SELECT ON $CLICKHOUSE_DATABASE.inner_view TO $CLICKHOUSE_VIEW_USER;
    CREATE MATERIALIZED VIEW test_mat_view TO inner_view DEFINER=$CLICKHOUSE_VIEW_USER SQL SECURITY DEFINER AS SELECT * FROM test_table_1;
"""

# View DEFINER has no source select access
$CLICKHOUSE_USER_CLIENT -q "INSERT INTO test_merge_explicit VALUES (3) -- { serverError ACCESS_DENIED }"

# View DEFINER has no write access
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $CLICKHOUSE_DATABASE.test_table_1 TO $CLICKHOUSE_VIEW_USER;"
$CLICKHOUSE_USER_CLIENT -q "INSERT INTO test_merge_explicit VALUES (3) -- { serverError ACCESS_DENIED }"

# Insert in the materialize view with sql security context
$CLICKHOUSE_CLIENT --multiline -q """
    GRANT INSERT ON $CLICKHOUSE_DATABASE.inner_view TO $CLICKHOUSE_VIEW_USER;
    GRANT SELECT ON $CLICKHOUSE_DATABASE.test_mat_view TO $CLICKHOUSE_USER;
"""
$CLICKHOUSE_USER_CLIENT --multiline -q """
    SELECT '-- insert into the materialized view with security context';
    INSERT INTO test_merge_explicit VALUES (7);
    SELECT * FROM test_mat_view ORDER BY ALL;
"""

# Access in chain of merge tables checked
$CLICKHOUSE_CLIENT --multiline -q """
    CREATE TABLE test_chain_table (key UInt32) Engine=MergeTree() ORDER BY key;
    CREATE TABLE test_merge_child Engine=Merge($CLICKHOUSE_DATABASE, 'test_chain_table', test_chain_table);
    CREATE TABLE test_merge_parent Engine=Merge($CLICKHOUSE_DATABASE, 'test_merge_child', 'test_merge_child');
    GRANT INSERT ON $CLICKHOUSE_DATABASE.test_merge_parent TO $CLICKHOUSE_USER;
    GRANT SELECT ON $CLICKHOUSE_DATABASE.test_merge_child TO $CLICKHOUSE_USER;
    GRANT INSERT,SELECT ON $CLICKHOUSE_DATABASE.test_chain_table TO $CLICKHOUSE_USER;
"""
$CLICKHOUSE_USER_CLIENT -q 'INSERT INTO test_merge_parent VALUES (1); -- { serverError ACCESS_DENIED }'
$CLICKHOUSE_CLIENT -q "GRANT INSERT ON $CLICKHOUSE_DATABASE.test_merge_child TO $CLICKHOUSE_USER;"
$CLICKHOUSE_USER_CLIENT --multiline -q """
    SELECT '-- insert in chain of merges';
    INSERT INTO test_merge_parent VALUES (1);
    SELECT * FROM test_chain_table ORDER BY ALL;
"""

# Complex case with every materialized view use separate user
#                                     |table3| <- |mat_view3|   |table6| <- |mat_view5|
#                                                      |                        |
# insert -> |merge1|  -> |table1| -> |mat_view2| -> |merge3| -> |merge4| -> |table4|
#             |                                                    |
#        |mat_view1| -> |merge2| -> |table2|     |table5| <- |mat_view4|

$CLICKHOUSE_CLIENT --multiline -q """
    CREATE TABLE complex_table_1 (a Int32) Engine=MergeTree() ORDER BY a;
    CREATE TABLE complex_table_2 (a Int64) Engine=MergeTree() ORDER BY a;
    CREATE TABLE complex_table_3 (a UInt32) Engine=MergeTree() ORDER BY a;
    CREATE TABLE complex_table_4 (a Int16) Engine=MergeTree() ORDER BY a;
    CREATE TABLE complex_table_5 (a Int128) Engine=MergeTree() ORDER BY a;
    CREATE TABLE complex_table_6 (a String) Engine=MergeTree() ORDER BY a;

    CREATE TABLE complex_merge_1 Engine=Merge($CLICKHOUSE_DATABASE, 'complex_table_1', complex_table_1);
    CREATE TABLE complex_merge_2 (a UInt64) Engine=Merge($CLICKHOUSE_DATABASE, 'complex_table_2', auto);
    CREATE TABLE complex_merge_3 (a UInt32) Engine=Merge($CLICKHOUSE_DATABASE, 'complex_merge_4', auto);
    CREATE TABLE complex_merge_4 (a Int32) Engine=Merge($CLICKHOUSE_DATABASE, 'complex_table_4', auto);

    CREATE USER ${CLICKHOUSE_DATABASE}_view_user_1 IDENTIFIED WITH plaintext_password BY 'user_password';
    GRANT SELECT ON $CLICKHOUSE_DATABASE.complex_merge_1 TO ${CLICKHOUSE_DATABASE}_view_user_1;
    CREATE MATERIALIZED VIEW complex_mat_view_1 TO complex_merge_2 DEFINER=${CLICKHOUSE_DATABASE}_view_user_1 SQL SECURITY DEFINER AS SELECT * FROM complex_merge_1;

    CREATE USER ${CLICKHOUSE_DATABASE}_view_user_2 IDENTIFIED WITH plaintext_password BY 'user_password';
    GRANT SELECT ON $CLICKHOUSE_DATABASE.complex_table_1 TO ${CLICKHOUSE_DATABASE}_view_user_2;
    CREATE MATERIALIZED VIEW complex_mat_view_2 TO complex_merge_3 DEFINER=${CLICKHOUSE_DATABASE}_view_user_2 SQL SECURITY DEFINER AS SELECT * FROM complex_table_1;

    CREATE USER ${CLICKHOUSE_DATABASE}_view_user_3 IDENTIFIED WITH plaintext_password BY 'user_password';
    GRANT SELECT ON $CLICKHOUSE_DATABASE.complex_merge_3 TO ${CLICKHOUSE_DATABASE}_view_user_3;
    CREATE MATERIALIZED VIEW complex_mat_view_3 TO complex_table_3 DEFINER=${CLICKHOUSE_DATABASE}_view_user_3 SQL SECURITY DEFINER AS SELECT * FROM complex_merge_3;

    CREATE USER ${CLICKHOUSE_DATABASE}_view_user_4 IDENTIFIED WITH plaintext_password BY 'user_password';
    GRANT SELECT ON $CLICKHOUSE_DATABASE.complex_merge_4 TO ${CLICKHOUSE_DATABASE}_view_user_4;
    CREATE MATERIALIZED VIEW complex_mat_view_4 TO complex_table_5 DEFINER=${CLICKHOUSE_DATABASE}_view_user_4 SQL SECURITY DEFINER AS SELECT * FROM complex_merge_4;

    CREATE USER ${CLICKHOUSE_DATABASE}_view_user_5 IDENTIFIED WITH plaintext_password BY 'user_password';
    GRANT SELECT ON $CLICKHOUSE_DATABASE.complex_table_4 TO ${CLICKHOUSE_DATABASE}_view_user_5;
    CREATE MATERIALIZED VIEW complex_mat_view_5 TO complex_table_6 DEFINER=${CLICKHOUSE_DATABASE}_view_user_5 SQL SECURITY DEFINER AS SELECT toString(a) a FROM complex_table_4;

    GRANT INSERT ON $CLICKHOUSE_DATABASE.complex_merge_1 TO $CLICKHOUSE_USER;
    GRANT INSERT ON $CLICKHOUSE_DATABASE.complex_table_1 TO $CLICKHOUSE_USER;

    SELECT '-- views permissions check';
"""

# Check INSERT grants: every grant missing on the way is reported, then given, until the insert succeeds.
# The order in which the missing grants are found is not specified, so the output is sorted.
for _ in $(seq 1 20); do
    if err=$($CLICKHOUSE_USER_CLIENT --send_logs_level=none -q "INSERT INTO complex_merge_1 VALUES (7)" 2>&1); then
        echo "inserted"
        break
    fi
    # With `auto`, a Merge table writes only into the tables that the definer of the materialized view can see.
    no_table=$(echo "$err" | grep -oE "Can't find any table to write for storage ${CLICKHOUSE_DATABASE}\.complex_merge_[0-9]" | head -n1 | grep -oE "complex_merge_[0-9]$" || true)
    if [ -n "$no_table" ]; then
        case "$no_table" in
            complex_merge_2) user=${CLICKHOUSE_DATABASE}_view_user_1; table=complex_table_2 ;;
            complex_merge_3) user=${CLICKHOUSE_DATABASE}_view_user_2; table=complex_merge_4 ;;
            complex_merge_4) user=${CLICKHOUSE_DATABASE}_view_user_2; table=complex_table_4 ;;
        esac
        echo "$user: no table to write for ${CLICKHOUSE_DATABASE}.$no_table"
        $CLICKHOUSE_CLIENT -q "GRANT INSERT ON $CLICKHOUSE_DATABASE.$table TO $user"
        continue
    fi
    user=$(echo "$err" | grep -oE "${CLICKHOUSE_DATABASE}_view_user_[0-9]+" | head -n1 || true)
    grant=$(echo "$err" | grep -oE "grant INSERT ON ${CLICKHOUSE_DATABASE}\.[a-z_0-9]+" | head -n1 | sed 's/^grant //' || true)
    if [ -z "$user" ] || [ -z "$grant" ]; then
        echo "Unexpected error: $err"
        break
    fi
    echo "$user: $grant"
    $CLICKHOUSE_CLIENT -q "GRANT $grant TO $user"
done | sed "s/${CLICKHOUSE_DATABASE}/default/g" | sort

$CLICKHOUSE_CLIENT --multiline -q """
    SELECT '-- check data written';
    SELECT _table, * FROM merge($CLICKHOUSE_DATABASE, 'complex_table_[1-5]') ORDER BY ALL;
    SELECT * FROM complex_table_6 ORDER BY ALL;
"""

$CLICKHOUSE_CLIENT --multiline -q """
    DROP VIEW test_mat_view SYNC;
    DROP USER $CLICKHOUSE_USER, $CLICKHOUSE_VIEW_USER;
    DROP VIEW complex_mat_view_1, complex_mat_view_2, complex_mat_view_3, complex_mat_view_4, complex_mat_view_5 SYNC;
    DROP USER ${CLICKHOUSE_DATABASE}_view_user_1, ${CLICKHOUSE_DATABASE}_view_user_2, ${CLICKHOUSE_DATABASE}_view_user_3, ${CLICKHOUSE_DATABASE}_view_user_4, ${CLICKHOUSE_DATABASE}_view_user_5;
"""
