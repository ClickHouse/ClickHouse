#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-ordinary-database
# Looks like server does not listen https port in fasttest
# FIXME Replicated database executes ALTERs in separate context, so transaction info is lost

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "drop table if exists tt"
$CLICKHOUSE_CLIENT -q "create table tt (n int) engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into tt values (1)"


tx 1 "begin transaction"
tx 1 "insert into tt values (2)"
tx 1 "alter table tt update n=n+100 where 1"
$CLICKHOUSE_CLIENT -q "alter table tt update n=n+42 where 1"
tx 1 "rollback"

$CLICKHOUSE_CLIENT -q "select 'after', n, _part from tt order by n"
