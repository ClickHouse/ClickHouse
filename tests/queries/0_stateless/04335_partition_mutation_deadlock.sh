#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database, no-ordinary-database

# Deadlock detection between transactional and non-transactional mutations
# (see `01168_mutations_isolation_3.sh`) must take partition-scoped
# (`IN PARTITION`) mutations into account: a cycle exists only when some
# partition is affected by the transaction's earlier mutation, the intermediate
# non-transactional mutation, and the transaction's current mutation at once.
# The first scenario checks that no spurious `Deadlock detected` is reported
# when there is no such common partition; the second scenario checks that a
# real deadlock through a common partition is still detected.

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

# A real partition-local deadlock must still be detected even when the
# transaction's mutation in the common partition is not its most recent one:
# the intermediate non-transactional partition-1 mutation waits for the
# transaction's first partition-1 mutation to commit, while the transaction's
# current partition-1 mutation waits for the intermediate one - a cycle through
# partition 1, although the transaction's most recent earlier mutation
# (partition 2) does not overlap the intermediate one.
$CLICKHOUSE_CLIENT -q "create table mt2 (p int, n int) engine=MergeTree partition by p order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into mt2 values (1, 1), (2, 1)"

tx 2 "begin transaction"
# earlier mutation of the transaction, partition 1
tx 2 "alter table mt2 update n=n+10 in partition 1 where 1"
# the most recent earlier mutation of the transaction, partition 2
tx 2 "alter table mt2 update n=n+10 in partition 2 where 1"
# intermediate non-transactional mutation, partition 1: waits for the
# transaction's first mutation to commit
$CLICKHOUSE_CLIENT -q "alter table mt2 update n=n+100 in partition 1 where 1"
# current mutation of the transaction, partition 1: waits for the intermediate
# one, so the deadlock must be detected
tx 2 "alter table mt2 update n=n+20 in partition 1 where 1" | grep -Eo "Deadlock detected" | uniq
tx 2 "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 2 "rollback"

wait_for_mutation "mt2" "mutation_4.txt"
$CLICKHOUSE_CLIENT -q "select 'after deadlock', p, n from mt2 order by p, n"
$CLICKHOUSE_CLIENT -q "drop table mt2"
