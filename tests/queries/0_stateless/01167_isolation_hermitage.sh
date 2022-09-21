#!/usr/bin/env bash
# Tags: long, no-fasttest, no-replicated-database
# Looks like server does not listen https port in fasttest
# FIXME Replicated database executes ALTERs in separate context, so transaction info is lost

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib
set -e

# https://github.com/ept/hermitage

$CLICKHOUSE_CLIENT -q "drop table if exists test"
$CLICKHOUSE_CLIENT -q "create table test (id int, value int) engine=MergeTree order by id"

function reset_table()
{
    $CLICKHOUSE_CLIENT -q "truncate table test;"
    $CLICKHOUSE_CLIENT -q "insert into test (id, value) values (1, 10);"
    $CLICKHOUSE_CLIENT -q "insert into test (id, value) values (2, 20);"
}

# TODO update test after implementing Read Committed

# G0
reset_table
tx 1 "begin transaction"
tx 2                                            "begin transaction"
tx 1 "alter table test update value=11 where id=1"
tx 2                                            "alter table test update value=12 where id=1" | grep -Eo "Serialization error" | uniq
tx 1 "alter table test update value=21 where id=2"
tx 1 "commit"
tx 2                                            "alter table test update value=22 where id=2" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 2                                            "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 2                                            "rollback"
$CLICKHOUSE_CLIENT -q "select 1, * from test order by id"

# G1a
reset_table
tx_async 3 "begin transaction"
tx_async 4                                           "begin transaction"
tx_async 3 "alter table test update value=101 where id=1"
tx_async 4                                           "select 2, * from test order by id"
tx_async 3 "alter table test update value=11 where id=1"
tx_async 3 "rollback"
tx_async 4                                           "select 3, * from test order by id"
tx_async 4                                           "commit"
tx_wait 3
tx_wait 4
$CLICKHOUSE_CLIENT -q "select 4, * from test order by id"

# G1b
reset_table
tx_async 5 "begin transaction"
tx_async 6                                           "begin transaction"
tx_async 5 "alter table test update value=101 where id=1"
tx_async 6                                           "select 5, * from test order by id"
tx_async 5 "alter table test update value=11 where id=1"
tx_async 5 "commit"
tx_async 6                                           "select 6, * from test order by id"
tx_async 6                                           "commit"
tx_wait 5
tx_wait 6
$CLICKHOUSE_CLIENT -q "select 7, * from test order by id"

# G1c
# NOTE both transactions will succeed if we implement skipping of unaffected partitions/parts
reset_table
tx 7 "begin transaction"
tx 8                                            "begin transaction"
tx 7 "alter table test update value=11 where id=1"
tx 8                                            "alter table test update value=22 where id=2" | grep -Eo "Serialization error" | uniq
tx 7 "select 8, * from test order by id"
tx 8                                            "select 9, * from test order by id" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 7 "commit"
tx 8                                            "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 8                                            "rollback"
$CLICKHOUSE_CLIENT -q "select 10, * from test order by id"

# OTV
reset_table
tx 9 "begin transaction"
tx 10                     "begin transaction"
tx 11                                         "begin transaction"
tx 9 "alter table test update value = 11 where id = 1"
tx 9 "alter table test update value = 19 where id = 2"
tx 10                     "alter table test update value = 12 where id = 1" | grep -Eo "Serialization error" | uniq
tx 9 "commit"
tx 11                                         "select 11, * from test order by id"
tx 10                     "alter table test update value = 18 where id = 2" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 11                                         "select 12, * from test order by id"
tx 10                     "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 10                     "rollback"
tx 11                                         "commit"
$CLICKHOUSE_CLIENT -q "select 13, * from test order by id"

# PMP
reset_table
tx_async 12 "begin transaction"
tx_async 13                                             "begin transaction"
tx_async 12 "select 14, * from test where value = 30"
tx_async 13                                             "insert into test (id, value) values (3, 30)"
tx_async 13                                             "commit"
tx_async 12 "select 15, * from test where value = 30"
tx_async 12 "commit"
tx_wait 12
tx_wait 13
$CLICKHOUSE_CLIENT -q "select 16, * from test order by id"

# PMP write
reset_table
tx 14 "begin transaction"
tx 15                                              "begin transaction"
tx 14 "alter table test update value = value + 10 where 1"
tx 15                                              "alter table test delete where value = 20" | grep -Eo "Serialization error" | uniq
tx 14 "commit"
tx 15                                              "select 17, * from test order by id" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 15                                              "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 15                                              "rollback"
$CLICKHOUSE_CLIENT -q "select 18, * from test order by id"

# P4
reset_table
tx 16 "begin transaction"
tx 17                                              "begin transaction"
tx 16 "select 19, * from test order by id"
tx 17                                              "select 20, * from test order by id"
tx 16 "alter table test update value = 11 where id = 1"
tx 17                                              "alter table test update value = 11 where id = 1" | grep -Eo "Serialization error" | uniq
tx 16 "commit"
tx 17                                              "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 17                                              "rollback"
$CLICKHOUSE_CLIENT -q "select 21, * from test order by id"

# G-single
reset_table
tx_async 18 "begin transaction"
tx_async 19                                              "begin transaction"
tx_sync  18 "select 22, * from test where id = 1"
tx_async 19                                              "select 23, * from test where id = 1"
tx_async 19                                              "select 24, * from test where id = 2"
tx_async 19                                              "alter table test update value = 12 where id = 1"
tx_async 19                                              "alter table test update value = 18 where id = 2"
tx_async 19                                              "commit"
tx_async 18 "select 25, * from test where id = 2"
tx_async 18 "commit"
tx_wait 18
tx_wait 19
$CLICKHOUSE_CLIENT -q "select 26, * from test order by id"

# G2
reset_table
tx_async 20 "begin transaction"
tx_async 21                                              "begin transaction"
tx_sync 20  "select 27, * from test where value % 3 = 0"
tx_async 21                                              "select 28, * from test where value % 3 = 0"
tx_async 20 "insert into test (id, value) values (3, 30)"
tx_async 21                                              "insert into test (id, value) values (4, 42)"
tx_async 20 "commit"
tx_async 21                                              "commit"
tx_wait 20
tx_wait 21
$CLICKHOUSE_CLIENT -q "select 29, * from test order by id"

