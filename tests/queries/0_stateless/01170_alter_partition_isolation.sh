#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Looks like server does not listen https port in fasttest
# FIXME Replicated database executes ALTERs in separate context, so transaction info is lost

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "drop table if exists mt"
$CLICKHOUSE_CLIENT -q "create table mt (n int) engine=MergeTree order by n"

tx 1 "begin transaction"
tx 1 "insert into mt values (1)"
tx 2                                            "begin transaction"
tx 2                                            "insert into mt values (2)"
tx 1 "select 1, n from mt order by n"
tx 1 "alter table mt drop partition id 'all'"
tx 2                                            "insert into mt values (4)"
tx 1 "insert into mt values (3)"
tx 1 "select 2, n from mt order by n"
tx 2                                            "select 3, n from mt order by n"
tx 2                                            "alter table mt drop partition id 'all'"
tx 2                                            "insert into mt values (5)"
tx 1 "select 4, n from mt order by n"
tx 2                                            "commit"
tx 1 "commit"

echo ''
$CLICKHOUSE_CLIENT -q "select 5, n from mt order by n"
echo ''

tx 4 "begin transaction"
tx 4 "insert into mt values (6)"
tx 3                                            "begin transaction"
tx 3                                            "insert into mt values (7)"
tx 4 "select 6, n from mt order by n"
tx 4 "alter table mt drop partition id 'all'"
tx 3                                            "insert into mt values (9)"
tx 4 "insert into mt values (8)"
tx 4 "select 7, n from mt order by n"
tx 3                                            "select 8, n from mt order by n"
tx 3                                            "alter table mt drop partition id 'all'" | grep -Eo "SERIALIZATION_ERROR" | uniq
tx 3                                            "insert into mt values (10)" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 4 "select 9, n from mt order by n"
tx 3                                            "rollback"
tx 4 "commit"

echo ''
$CLICKHOUSE_CLIENT -q "select 10, n from mt order by n"
echo ''

$CLICKHOUSE_CLIENT -q "drop table if exists another_mt"
$CLICKHOUSE_CLIENT -q "create table another_mt (n int) engine=MergeTree order by n"

tx 5 "begin transaction"
tx 5 "insert into another_mt values (11)"
tx 6                                            "begin transaction"
tx 6                                            "insert into mt values (12)"
tx 6                                            "insert into another_mt values (13)"
tx 5 "alter table another_mt move partition id 'all' to table mt"
tx 6                                            "alter table another_mt replace partition id 'all' from mt"
tx 5 "alter table another_mt attach partition id 'all' from mt"
tx 5 "commit"
tx 6                                            "commit"

$CLICKHOUSE_CLIENT -q "select 11, n from mt order by n"
$CLICKHOUSE_CLIENT -q "select 12, n from another_mt order by n"

$CLICKHOUSE_CLIENT -q "drop table another_mt"
$CLICKHOUSE_CLIENT -q "drop table mt"
