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
$CLICKHOUSE_CLIENT -q "create table mt (n int) engine=MergeTree order by tuple()"

$CLICKHOUSE_CLIENT -q "insert into mt values (1)"

tx 1 "begin transaction"
tx 2                                            "begin transaction"
tx 1 "insert into mt values (2)"
tx 2                                            "insert into mt values (3)"
tx 2                                            "alter table mt update n=n*10 where 1 settings mutations_sync=1"
tx 2                                            "select 1, n, _part from mt order by n"
tx 1 "select 2, n, _part from mt order by n"
tx 1 "alter table mt update n=n+1 where 1 settings mutations_sync=1" | grep -Eo "Serialization error" | uniq
tx 1 "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 2                                            "rollback"


tx 3 "begin transaction"
tx 3 "select 3, n, _part from mt order by n"
tx 4                                            "begin transaction"
tx 3 "insert into mt values (2)"
tx 4                                            "insert into mt values (3)"
tx 4                                            "alter table mt update n=n*2 where 1"
tx 3 "alter table mt update n=n+42 where 1"
tx 3 "insert into mt values (4)"
tx 4                                            "insert into mt values (5)"
tx 3 "commit" | grep -Eo "Serialization error" | uniq
tx 4                                            "commit"


tx 5 "begin transaction"
tx 5 "select 4, n, _part from mt order by n"
tx 6                                            "begin transaction"
tx 6                                            "alter table mt delete where n%2=1 settings mutations_sync=1"
tx 5 "select 5, n, _part from mt order by n"
tx 5 "alter table mt drop partition id 'all'" | grep -Eo "SERIALIZATION_ERROR" | uniq
tx 6                                            "select 6, n, _part from mt order by n"
tx 5 "rollback"
tx 6                                            "insert into mt values (8)"
tx 6                                            "alter table mt update n=n*10 where 1"
tx 6                                            "commit"

tx 8 "begin transaction"
tx 8 "select 7, n, _part from mt order by n"

$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=0 -q "drop table mt"
