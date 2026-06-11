#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database, no-ordinary-database

# With partition-scoped (`IN PARTITION`) mutations a transaction must not get a
# spurious `Deadlock detected` when the intermediate non-transactional mutation
# touches a different partition than the transaction's earlier mutation.
# A cycle requires the intermediate mutation to overlap both the current and the
# earlier mutation; here the earlier mutation is in partition 2 and the
# intermediate one is in partition 1, so there is no cycle and the transaction
# must commit successfully.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

$CLICKHOUSE_CLIENT -q "create table mt (p int, n int) engine=MergeTree partition by p order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into mt values (1, 1), (2, 1)"

tx 1 "begin transaction"
# earlier mutation of the transaction, partition 2
tx 1 "alter table mt update n=n+10 in partition 2 where 1"
# intermediate non-transactional mutation, partition 1 (different partition)
$CLICKHOUSE_CLIENT -q "alter table mt update n=n+100 in partition 1 where 1"
# current mutation of the transaction, partition 1: overlaps the intermediate one
# but not the earlier one, so there must be no deadlock
tx 1 "alter table mt update n=n+20 in partition 1 where 1" | grep -Eo "Deadlock detected" | uniq
tx 1 "commit" | grep -Eo "INVALID_TRANSACTION" | uniq

wait_for_mutation "mt" "mutation_3.txt"
$CLICKHOUSE_CLIENT -q "select 'after', p, n from mt order by p, n"
$CLICKHOUSE_CLIENT -q "drop table mt"
