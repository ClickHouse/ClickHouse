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

    $CLICKHOUSE_CLIENT -q "insert into $table values (1), (2), (3)" # inserts all_1_1_0
}

function concurrent_delete_before()
{
    echo "concurrent_delete_before"

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
    echo "concurrent_delete_after"

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
    echo "concurrent_delete_rollback"

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
