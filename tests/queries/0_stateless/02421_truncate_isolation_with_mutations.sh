#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-ordinary-database, long

set -e -o pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib
# shellcheck source=./parts.lib
. "$CURDIR"/parts.lib

CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --apply_mutations_on_fly 0"

function reset_table()
{
    table=${1:-"tt"}
    settings=${2:-""}
    $CLICKHOUSE_CLIENT -q "drop table if exists $table"
    $CLICKHOUSE_CLIENT -q "create table $table (n int) engine=MergeTree order by tuple() $settings"

    $CLICKHOUSE_CLIENT -q "insert into $table values (1), (2), (3)" # inserts all_1_1_0
}

function concurrent_delete_before()
{
    $CLICKHOUSE_CLIENT -q "select 'concurrent_delete_before'"

    reset_table tt

    tx 11 "begin transaction"
    tx 11 "select 41, count() from tt"
    tx 12                                            "begin transaction"
    tx 12                                            "alter table tt delete where n%2=1"
    tx 11 "select 41, count() from tt"
    tx 11 "truncate table tt" | grep -Eo "SERIALIZATION_ERROR" | uniq
    tx 12                                            "select 42, count() from tt"
    tx 11 "rollback"
    tx 12                                            "insert into tt values (4)"
    tx 12                                            "commit"

    $CLICKHOUSE_CLIENT -q "select n from tt order by n"
}

concurrent_delete_before

function concurrent_delete_after()
{
    $CLICKHOUSE_CLIENT -q "select 'concurrent_delete_after'"

    reset_table tt

    tx 21 "begin transaction"
    tx 22                                            "begin transaction"
    tx 21 "select 111, count() from tt"
    tx 21 "truncate table tt"
    tx 22                                            "select 112, count() from tt"
    tx 22                                            "alter table tt delete where n%2=1" | grep -Eo "UNFINISHED" | uniq
    tx 21 "commit"
    tx 22                                            "rollback"

    $CLICKHOUSE_CLIENT -q "select n from tt order by n"
}

concurrent_delete_after

function concurrent_delete_rollback()
{
    $CLICKHOUSE_CLIENT -q "select 'concurrent_delete_rollback'"

    reset_table tt

    tx 31 "begin transaction"
    tx 31 "select count() from tt"
    tx 32                                            "begin transaction"
    tx 32                                            "alter table tt delete where n%2=1"
    tx 31 "select count() from tt"
    tx 32                                            "select count() from tt"
    tx 31 "select count() from tt"
    tx 32                                            "rollback"
    tx 31 "truncate table tt"
    tx 31 "commit"

    $CLICKHOUSE_CLIENT -q "select count() from tt"
}

concurrent_delete_rollback


function concurrent_optimize_table_not_start()
{
    $CLICKHOUSE_CLIENT -q "select 'concurrent_optimize_table_not_start'"

    reset_table tt

    tx 41 "begin transaction"
    tx 41 "insert into tt values (4)" # inserts all_2_2_0

    tx 42             "begin transaction"
    tx 42             "optimize table tt final"
    tx 42             "commit"

    tx 41 "select count() from tt"
    tx 41 "commit"

    $CLICKHOUSE_CLIENT -q "select count(), _part from tt group by _part order by _part"
}

concurrent_optimize_table_not_start


function concurrent_optimize_table()
{
    $CLICKHOUSE_CLIENT -q "select 'concurrent_optimize_table'"

    reset_table tt

    $CLICKHOUSE_CLIENT -q "insert into $table values (4), (5)" # inserts all_2_2_0

    tx 41 "begin transaction"
    tx 41 "optimize table tt final"

    tx 42                "begin transaction"
    tx 42                "insert into tt values (6)" # inserts all_3_3_0

    tx 43                                            "begin transaction"
    tx 43                                            "select count() from tt"
    tx 43                                            "alter table tt drop partition id 'all'" | grep -Eo "SERIALIZATION_ERROR" | uniq

    tx 42                "commit"
    tx 43                                            "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
    tx 41 "commit"

    $CLICKHOUSE_CLIENT -q "select count(), _part from tt group by _part order by _part"
}

concurrent_optimize_table

function concurrent_optimize_table_before()
{
    $CLICKHOUSE_CLIENT -q "select 'concurrent_optimize_table_before'"

    reset_table tt

    tx 51 "begin transaction"
    tx 52             "begin transaction"
    tx 51 "optimize table tt final" # inserts all_1_1_1
    tx 51 "rollback" # inserts all_1_1_1 is outdated
    tx 52             "alter table tt drop partition id 'all'" | grep -vwe "PART_IS_TEMPORARILY_LOCKED" ||: # conflict with all_1_1_1
    tx 52             "rollback"

    $CLICKHOUSE_CLIENT -q "select count(), _part from tt group by _part order by _part"
}

concurrent_optimize_table_before

function drop_parts_which_already_outdated()
{
    $CLICKHOUSE_CLIENT -q "select 'drop_parts_which_already_outdated'"

    reset_table tt "settings old_parts_lifetime=0"

    $CLICKHOUSE_CLIENT -q "optimize table tt final /*all_1_1_1*/"
    $CLICKHOUSE_CLIENT -q "optimize table tt final /*all_1_1_2*/"
    $CLICKHOUSE_CLIENT -q "optimize table tt final /*all_1_1_3*/"
    $CLICKHOUSE_CLIENT -q "optimize table tt final /*all_1_1_4*/"
    $CLICKHOUSE_CLIENT -q "optimize table tt final /*all_1_1_5*/"
    $CLICKHOUSE_CLIENT -q "optimize table tt final /*all_1_1_6*/"

    $CLICKHOUSE_CLIENT -q "insert into $table values (4)" # inserts all_2_2_0

    tx 69             "begin transaction"
    tx 69             "select 'before optimize', count(), _part from tt group by _part order by _part"

    tx 61 "begin transaction"
    tx 61 "optimize table tt final /*all_1_2_7*/"
    tx 61 "commit"

    tx 62 "begin transaction"
    tx 62 "optimize table tt final /*all_1_2_8*/"

    tx 69             "select 'after optimize', count(), _part from tt group by _part order by _part"
    tx 69             "alter table tt drop partition id 'all'" | grep -Eo "SERIALIZATION_ERROR" | uniq
    tx 69             "rollback"

    tx 62 "rollback"

    $CLICKHOUSE_CLIENT -q "select 'at the end', count(), _part from tt group by _part order by _part"
}

drop_parts_which_already_outdated

function unable_drop_one_part_which_outdated_but_visible()
{
    $CLICKHOUSE_CLIENT -q "select 'unable_drop_one_part_which_outdated_but_visible'"

    reset_table tt "settings old_parts_lifetime=0"

    $CLICKHOUSE_CLIENT -q "optimize table tt final /*all_1_1_1*/"
    $CLICKHOUSE_CLIENT -q "optimize table tt final /*all_1_1_2*/"

    $CLICKHOUSE_CLIENT -q "insert into $table values (4)" # inserts all_2_2_0

    tx 79             "begin transaction"
    tx 79             "select 'before optimize', count(), _part from tt group by _part order by _part"

    tx 71 "begin transaction"
    tx 71 "optimize table tt final /*all_1_2_3*/"

    tx 79             "select 'after optimize', count(), _part from tt group by _part order by _part"
    tx 79             "alter table tt drop part 'all_2_2_0'" | grep -Eo "NO_SUCH_DATA_PART" | uniq
    tx 79             "rollback"

    tx 71 "rollback"

    $CLICKHOUSE_CLIENT -q "select 'at the end', count(), _part from tt group by _part order by _part"
}

unable_drop_one_part_which_outdated_but_visible

function drop_one_part_which_outdated_and_reverted()
{
    $CLICKHOUSE_CLIENT -q "select 'drop_one_part_which_outdated_and_reverted'"

    reset_table tt "settings old_parts_lifetime=0"

    $CLICKHOUSE_CLIENT -q "optimize table tt final /*all_1_1_1*/"

    $CLICKHOUSE_CLIENT -q "insert into $table values (4)" # inserts all_2_2_0

    tx 89             "begin transaction"
    tx 89             "select 'before optimize', count(), _part from tt group by _part order by _part"

    tx 81 "begin transaction"
    tx 81 "optimize table tt final /*all_1_2_2*/"

    tx 89             "select 'after optimize', count(), _part from tt group by _part order by _part"
    tx 81 "rollback"

    tx 89             "select 'after rollback', count(), _part from tt group by _part order by _part"
    tx 89             "alter table tt drop part 'all_2_2_0'"
    tx 89             "commit"

    $CLICKHOUSE_CLIENT -q "select 'at the end', count(), _part from tt group by _part order by _part"
}

drop_one_part_which_outdated_and_reverted

function drop_one_part_which_outdated_and_reverted_no_name_intersection()
{
    $CLICKHOUSE_CLIENT -q "select 'drop_one_part_which_outdated_and_reverted_no_name_intersection'"

    reset_table tt "settings old_parts_lifetime=0"

    $CLICKHOUSE_CLIENT -q "insert into $table values (4)" # inserts all_2_2_0

    tx 99             "begin transaction"
    tx 99             "select 'before optimize', count(), _part from tt group by _part order by _part"

    tx 91 "begin transaction"
    tx 91 "optimize table tt final /*all_1_2_1*/"

    tx 99             "select 'after optimize', count(), _part from tt group by _part order by _part"
    tx 91 "rollback"

    tx 99             "select 'after rollback', count(), _part from tt group by _part order by _part"
    tx 99             "alter table tt drop part 'all_2_2_0'"
    tx 99             "commit"

    $CLICKHOUSE_CLIENT -q "select 'at the end', count(), _part from tt group by _part order by _part"
}

drop_one_part_which_outdated_and_reverted_no_name_intersection
