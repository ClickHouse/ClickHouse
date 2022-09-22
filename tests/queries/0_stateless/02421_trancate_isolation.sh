#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-ordinary-database

set -e -o pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib


function reset_table()
{
    table=${1:-"tt"}
    $CLICKHOUSE_CLIENT -q "drop table if exists $table"
    $CLICKHOUSE_CLIENT -q "create table $table (n int) engine=MergeTree order by tuple()"

    $CLICKHOUSE_CLIENT -q "insert into $table values (1)" # inserts all_1_1_0
    $CLICKHOUSE_CLIENT -q "insert into $table values (2)" # inserts all_2_2_0
    $CLICKHOUSE_CLIENT -q "insert into $table values (3)" # inserts all_3_3_0
}

function concurrent_insert()
{
    echo "concurrent_insert"

    reset_table

    tx 1 "begin transaction"
    tx 2                                            "begin transaction"
    tx 1 "insert into tt values (1)"                                               # inserts all_4_4_0
    tx 2                                            "insert into tt values (2)"    # inserts all_5_5_0
    tx 1 "insert into tt values (3)"                                               # inserts all_6_6_0
    tx 1 "truncate table tt"                                                       # creates all_1_4_1 all_6_6_1
    tx 1 "commit"
    tx 2                                            "commit"

    $CLICKHOUSE_CLIENT -q "select n from tt order by n"
    $CLICKHOUSE_CLIENT -q "select name, rows from system.parts
                              where table='tt' and database=currentDatabase() and active
                              order by name"
}

concurrent_insert

function concurrent_drop_part_before()
{
    echo "concurrent_drop_part_before"

    reset_table

    tx 11 "begin transaction"
    tx 22                         "begin transaction"
    tx 22                         "alter table tt drop part 'all_2_2_0'"
    tx 11 "truncate table tt" | grep -Eo "SERIALIZATION_ERROR" | uniq
    tx 11 "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
    tx 22                         "commit"

    $CLICKHOUSE_CLIENT -q "select n from tt order by n"
    $CLICKHOUSE_CLIENT -q "select name, rows from system.parts
                              where table='tt' and database=currentDatabase() and active
                              order by name"
}

concurrent_drop_part_before

function concurrent_drop_part_after()
{
    echo "concurrent_drop_part_after"

    reset_table drop_part_after_table

    tx 31 "begin transaction"
    tx 32             "begin transaction"
    tx 31 "truncate table drop_part_after_table"
    tx 32             "alter table drop_part_after_table drop part 'all_2_2_0'" | grep -Eo "NO_SUCH_DATA_PART" | uniq
    tx 31 "commit"
    tx 32             "commit" | grep -Eo "INVALID_TRANSACTION" | uniq

    $CLICKHOUSE_CLIENT -q "select n from drop_part_after_table order by n"
    $CLICKHOUSE_CLIENT -q "select name, rows from system.parts
                              where table='drop_part_after_table' and database=currentDatabase() and active
                              order by name"
    $CLICKHOUSE_CLIENT -q "system flush logs"
    $CLICKHOUSE_CLIENT -q "select event_type, part_name from system.part_log
                              where table='drop_part_after_table' and database=currentDatabase()
                              order by part_name"
}

concurrent_drop_part_after

function concurrent_delete()
{
    echo "concurrent_delete"

    reset_table

    tx 41 "begin transaction"
    tx 41 "select 41, count() from tt"
    tx 42                                            "begin transaction"
    tx 42                                            "alter table tt delete where n%2=1"
    tx 41 "select 41, count() from tt"
    tx 41 "truncate table tt" | grep -Eo "SERIALIZATION_ERROR" | uniq
    tx 42                                            "select 42, count() from tt"
    tx 41 "rollback"
    tx 42                                            "insert into tt values (4)"
    tx 42                                            "commit"

    $CLICKHOUSE_CLIENT -q "select n from tt order by n"
}

concurrent_delete

function concurrent_delete_rollback()
{
    echo "concurrent_delete_rollback"

    reset_table

    tx 51 "begin transaction"
    tx 51 "select count() from tt"
    tx 52                                            "begin transaction"
    tx 52                                            "alter table tt delete where n%2=1"
    tx 51 "select count() from tt"
    tx 52                                            "select count() from tt"
    tx 51 "select count() from tt"
    tx 52                                            "rollback"
    tx 51 "truncate table tt"
    tx 51 "commit"

    $CLICKHOUSE_CLIENT -q "select count() from tt"
}

concurrent_delete_rollback

function read_from_snapshot()
{
    echo "read_from_snapshot"

    reset_table

    tx 61 "begin transaction"
    tx 61 "select count() from tt"
    tx 62                                            "begin transaction"
    tx 62                                            "truncate table tt"
    tx 61 "select count() from tt"
    tx 62                                            "select count() from tt"
    tx 62                                            "commit"
    tx 61 "select count() from tt"
    tx 61 "commit"

    $CLICKHOUSE_CLIENT -q "select count() from tt"
}

read_from_snapshot
