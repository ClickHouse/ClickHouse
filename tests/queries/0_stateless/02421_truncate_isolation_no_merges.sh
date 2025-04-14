#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-ordinary-database, long

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

    # In order to preserve parts names merges have to be disabled
    $CLICKHOUSE_CLIENT -q "system stop merges $table"

    $CLICKHOUSE_CLIENT -q "insert into $table values (1)" # inserts all_1_1_0
    $CLICKHOUSE_CLIENT -q "insert into $table values (2)" # inserts all_2_2_0
    $CLICKHOUSE_CLIENT -q "insert into $table values (3)" # inserts all_3_3_0
}

function concurrent_drop_after()
{
    echo "concurrent_drop_after"

    reset_table

    tx 11 "begin transaction"
    tx 11 "select count() from tt"
    tx 11 "truncate table tt"
    $CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=0 -q "drop table tt"
    tx 11 "commit"
}

concurrent_drop_after

function concurrent_drop_before()
{
    echo "concurrent_drop_before"

    reset_table

    tx 21 "begin transaction"
    tx 21 "select count() from tt"
    $CLICKHOUSE_CLIENT -q                                 "drop table tt"
    tx 21 "truncate table tt" | grep -Eo "UNKNOWN_TABLE" | uniq
    tx 21 "rollback"
}

concurrent_drop_before

function concurrent_insert()
{
    echo "concurrent_insert"

    reset_table

    tx 31 "begin transaction"
    tx 32                                            "begin transaction"
    tx 31 "insert into tt values (1)"                                               # inserts all_4_4_0
    tx 32                                            "insert into tt values (2)"    # inserts all_5_5_0
    tx 31 "insert into tt values (3)"                                               # inserts all_6_6_0
    tx 31 "truncate table tt"                                                       # creates all_1_4_1 all_6_6_1
    tx 31 "commit"
    tx 32                                            "commit"

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

    tx 41 "begin transaction"
    tx 42                         "begin transaction"
    tx 42                         "alter table tt drop part 'all_2_2_0'"
    tx 41 "truncate table tt" | grep -Eo "SERIALIZATION_ERROR" | uniq
    tx 41 "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
    tx 42                         "commit"

    $CLICKHOUSE_CLIENT -q "select n from tt order by n"
    $CLICKHOUSE_CLIENT -q "select name, rows from system.parts
                              where table='tt' and database=currentDatabase() and active
                              order by name"

    reset_table
}

concurrent_drop_part_before

function read_from_snapshot()
{
    echo "read_from_snapshot"

    reset_table

    tx 51 "begin transaction"
    tx 51 "select count() from tt"
    tx 52                                            "begin transaction"
    tx 52                                            "truncate table tt"
    tx 51 "select count() from tt"
    tx 52                                            "select count() from tt"
    tx 52                                            "commit"
    tx 51 "select count() from tt"
    tx 51 "commit"

    $CLICKHOUSE_CLIENT -q "select count() from tt"
}

read_from_snapshot


function concurrent_drop_part_after()
{
    echo "concurrent_drop_part_after"

    reset_table drop_part_after_table

    tx 61 "begin transaction"
    tx 62             "begin transaction"
    tx 61 "truncate table drop_part_after_table"
    tx 62             "alter table drop_part_after_table drop part 'all_2_2_0'" | grep -Eo "NO_SUCH_DATA_PART" | uniq
    tx 61 "commit"
    tx 62             "commit" | grep -Eo "INVALID_TRANSACTION" | uniq

    $CLICKHOUSE_CLIENT -q "select n from drop_part_after_table order by n"
    $CLICKHOUSE_CLIENT -q "select name, rows from system.parts
                              where table='drop_part_after_table' and database=currentDatabase() and active
                              order by name"
    $CLICKHOUSE_CLIENT -q "system flush logs part_log"
    $CLICKHOUSE_CLIENT -q "select event_type, part_name from system.part_log
                              where table='drop_part_after_table' and database=currentDatabase()
                              order by part_name"
}

concurrent_drop_part_after

function concurrent_truncate_notx_after()
{
    echo "concurrent_truncate_notx_after"

    reset_table

    tx 71 "begin transaction"
    tx 71 "select count() from tt"
    tx 71 "alter table tt drop part 'all_2_2_0'"
    $CLICKHOUSE_CLIENT -q                                 "truncate table tt"
    # return 0, since truncate was out of transaction
    # it would be better if exception raised
    tx 71 "select count() from tt"
    tx 71 "commit"

    $CLICKHOUSE_CLIENT -q "select count() from tt"
}

concurrent_truncate_notx_after

function concurrent_truncate_notx_before()
{
    echo "concurrent_truncate_notx_before"

    reset_table

    tx 81 "begin transaction"
    tx 81 "select count() from tt"
    $CLICKHOUSE_CLIENT -q                                 "truncate table tt"
    tx 81 "alter table tt drop part 'all_2_2_0'" | grep -Eo "NO_SUCH_DATA_PART" | uniq
    tx 81 "select count() from tt" | grep -Eo "INVALID_TRANSACTION" | uniq
    tx 81 "commit" | grep -Eo "INVALID_TRANSACTION" | uniq

    $CLICKHOUSE_CLIENT -q "select count() from tt"
}

concurrent_truncate_notx_before

function concurrent_rollback_truncate()
{
    echo "concurrent_rollback_truncate"

    reset_table

    tx 91       "begin transaction"
    tx 92               "begin transaction"
    tx 91       "truncate table tt"
    tx_async 91 "rollback"
    tx 92               "truncate table tt" | grep -vwe "PART_IS_TEMPORARILY_LOCKED" -vwe "SERIALIZATION_ERROR" ||:
    tx 92               "rollback"
    tx_wait 91

    $CLICKHOUSE_CLIENT -q "select count() from tt"
}

concurrent_rollback_truncate
