#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-ordinary-database, no-encrypted-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

# test deadlock1
$CLICKHOUSE_CLIENT -q "create table tt (n int) engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into tt values (1)"


$CLICKHOUSE_CLIENT -q "alter table tt update n=n+100 where 1"
tx 1 "begin transaction"
tx 1 "alter table tt update n=n+10 where 1"
tx 1 "commit"

wait_for_mutation "tt" "mutation_3.txt"

$CLICKHOUSE_CLIENT -q "select 'after', n, _part from tt order by n"

$CLICKHOUSE_CLIENT -q "drop table tt"

# test deadlock2
$CLICKHOUSE_CLIENT -q "create table tt (n int) engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into tt values (1)"


$CLICKHOUSE_CLIENT -q "alter table tt update n=n+100 where 1"
$CLICKHOUSE_CLIENT -q "alter table tt update n=n+200 where 1"
tx 2 "begin transaction"
tx 2 "alter table tt update n=n+10 where 1"
tx 2 "commit"

wait_for_mutation "tt" "mutation_4.txt"

$CLICKHOUSE_CLIENT -q "select 'after', n, _part from tt order by n"

$CLICKHOUSE_CLIENT -q "drop table tt"

# test deadlock3
$CLICKHOUSE_CLIENT -q "create table tt (n int) engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into tt values (1)"


$CLICKHOUSE_CLIENT -q "alter table tt update n=n+100 where 1"
tx 3 "begin transaction"
tx 3 "alter table tt update n=n+10 where 1"
tx 3 "commit"
tx 4 "begin transaction"
tx 4 "alter table tt update n=n+20 where 1"
tx 4 "commit"

wait_for_mutation "tt" "mutation_4.txt"

$CLICKHOUSE_CLIENT -q "select 'after', n, _part from tt order by n"

$CLICKHOUSE_CLIENT -q "drop table tt"

# test deadlock4
$CLICKHOUSE_CLIENT -q "create table tt (n int) engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into tt values (1)"


tx 5 "begin transaction"
tx 5 "alter table tt update n=n+10 where 1"
tx 5 "commit"
$CLICKHOUSE_CLIENT -q "alter table tt update n=n+100 where 1"
tx 6 "begin transaction"
tx 6 "alter table tt update n=n+20 where 1"
tx 6 "commit"

wait_for_mutation "tt" "mutation_4.txt"

$CLICKHOUSE_CLIENT -q "select 'after', n, _part from tt order by n"

$CLICKHOUSE_CLIENT -q "drop table tt"

# test deadlock5
$CLICKHOUSE_CLIENT -q "create table tt (n int) engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into tt values (1)"


tx 7 "begin transaction"
tx 7 "alter table tt update n=n+10 where 1"
$CLICKHOUSE_CLIENT -q "alter table tt update n=n+100 where 1"
tx 7 "alter table tt update n=n+30 where 1" | grep -Eo "Deadlock detected" | uniq
tx 7 "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 7 "rollback"

wait_for_mutation "tt" "mutation_3.txt"

$CLICKHOUSE_CLIENT -q "select 'after', n, _part from tt order by n"

$CLICKHOUSE_CLIENT -q "drop table tt"

# test deadlock6
$CLICKHOUSE_CLIENT -q "create table tt (n int) engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into tt values (1)"


tx 8 "begin transaction"
tx 8 "alter table tt update n=n+10 where 1"
tx 8 "alter table tt update n=n+20 where 1"
$CLICKHOUSE_CLIENT -q "alter table tt update n=n+100 where 1"
tx 8 "alter table tt update n=n+30 where 1" | grep -Eo "Deadlock detected" | uniq
tx 8 "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 8 "rollback"

wait_for_mutation "tt" "mutation_4.txt"

$CLICKHOUSE_CLIENT -q "select 'after', n, _part from tt order by n"

$CLICKHOUSE_CLIENT -q "drop table tt"
