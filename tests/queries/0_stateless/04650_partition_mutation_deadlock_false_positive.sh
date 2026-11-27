#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database, no-ordinary-database

# Regression test for a false `Deadlock detected` between transactional and
# non-transactional partition-scoped (`IN PARTITION`) mutations. A mutation waits
# for another one only while some visible part still has to be processed by it, so
# the deadlock check validates each wait edge separately. When there is nothing to
# mutate at all (the affected partition is empty), no mutation waits for another
# one and the chain must simply complete.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

$CLICKHOUSE_CLIENT -q "create table mt (p int, n int) engine=MergeTree partition by p order by tuple()"

tx 1 "begin transaction"
# earlier mutation of the transaction, partition 1 (the table is empty, so there is nothing to mutate)
tx 1 "alter table mt update n=n+1 in partition 1 where 1"
# intermediate non-transactional mutation, partition 1: its (finished) entry is retained
# by `finished_mutations_to_keep`, so it stays between the two transactional mutations
$CLICKHOUSE_CLIENT -q "alter table mt update n=n+10 in partition 1 where 1"
# current mutation of the transaction, partition 1: it overlaps both of them, but no part
# is waiting anywhere, so there must be no deadlock
tx 1 "alter table mt update n=n+100 in partition 1 where 1" | grep -Eo "Deadlock detected" | uniq
tx 1 "commit" | grep -Eo "INVALID_TRANSACTION" | uniq

wait_for_all_mutations "mt"
$CLICKHOUSE_CLIENT -q "select 'unfinished mutations', count() from system.mutations where database=currentDatabase() and table='mt' and not is_done"
$CLICKHOUSE_CLIENT -q "drop table mt"
