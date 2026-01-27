#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database, no-ordinary-database, no-encrypted-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

# test deadlock1
$CLICKHOUSE_CLIENT -q "create table mt (n int) engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into mt values (1)"


$CLICKHOUSE_CLIENT -q "alter table mt update n=n+100 where 1"
tx 1 "begin transaction"
tx 1 "alter table mt update n=n+10 where 1"
tx 1 "commit"

wait_for_mutation "mt" "mutation_3.txt"
$CLICKHOUSE_CLIENT -q "select 'after', n, _part from mt order by n"
$CLICKHOUSE_CLIENT -q "drop table mt"

# test deadlock2
$CLICKHOUSE_CLIENT -q "create table mt (n int) engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into mt values (1)"


$CLICKHOUSE_CLIENT -q "alter table mt update n=n+100 where 1"
$CLICKHOUSE_CLIENT -q "alter table mt update n=n+200 where 1"
tx 2 "begin transaction"
tx 2 "alter table mt update n=n+10 where 1"
tx 2 "commit"

wait_for_mutation "mt" "mutation_4.txt"
$CLICKHOUSE_CLIENT -q "select 'after', n, _part from mt order by n"
$CLICKHOUSE_CLIENT -q "drop table mt"

# test deadlock3
$CLICKHOUSE_CLIENT -q "create table mt (n int) engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into mt values (1)"


$CLICKHOUSE_CLIENT -q "alter table mt update n=n+100 where 1"
tx 3 "begin transaction"
tx 3 "alter table mt update n=n+10 where 1"
tx 3 "commit"
tx 4 "begin transaction"
tx 4 "alter table mt update n=n+20 where 1"
tx 4 "commit"

wait_for_mutation "mt" "mutation_4.txt"
$CLICKHOUSE_CLIENT -q "select 'after', n, _part from mt order by n"
$CLICKHOUSE_CLIENT -q "drop table mt"

# test deadlock4
$CLICKHOUSE_CLIENT -q "create table mt (n int) engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into mt values (1)"


tx 5 "begin transaction"
tx 5 "alter table mt update n=n+10 where 1"
tx 5 "commit"
$CLICKHOUSE_CLIENT -q "alter table mt update n=n+100 where 1"
tx 6 "begin transaction"
tx 6 "alter table mt update n=n+20 where 1"
tx 6 "commit"

wait_for_mutation "mt" "mutation_4.txt"
$CLICKHOUSE_CLIENT -q "select 'after', n, _part from mt order by n"
$CLICKHOUSE_CLIENT -q "drop table mt"

# test deadlock5
$CLICKHOUSE_CLIENT -q "create table mt (n int) engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into mt values (1)"


tx 7 "begin transaction"
tx 7 "alter table mt update n=n+10 where 1"
$CLICKHOUSE_CLIENT -q "alter table mt update n=n+100 where 1"
tx 7 "alter table mt update n=n+30 where 1" | grep -Eo "Deadlock detected" | uniq
tx 7 "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 7 "rollback"

wait_for_mutation "mt" "mutation_3.txt"
$CLICKHOUSE_CLIENT -q "select 'after', n, _part from mt order by n"
$CLICKHOUSE_CLIENT -q "drop table mt"

# test deadlock6
$CLICKHOUSE_CLIENT -q "create table mt (n int) engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into mt values (1)"


tx 8 "begin transaction"
tx 8 "alter table mt update n=n+10 where 1"
tx 8 "alter table mt update n=n+20 where 1"
$CLICKHOUSE_CLIENT -q "alter table mt update n=n+100 where 1"
tx 8 "alter table mt update n=n+30 where 1" | grep -Eo "Deadlock detected" | uniq
tx 8 "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 8 "rollback"

wait_for_mutation "mt" "mutation_4.txt"
$CLICKHOUSE_CLIENT -q "select 'after', n, _part from mt order by n"
$CLICKHOUSE_CLIENT -q "drop table mt"

# test deadlock7: intermediate mutation with transaction
# Deadlock detected before Serialization error.
$CLICKHOUSE_CLIENT -q "create table mt (n int) engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into mt values (1)"

tx 9 "begin transaction"
tx 10                                            "begin transaction"
tx 10                                            "alter table mt update n=n+10 where 1"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES mt;"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT storage_shared_merge_tree_mutate_pause_before_wait;"
tx_async 9 "alter table mt update n=n+100 where 1" >/dev/null

# Wait for all mutations to be created in system.mutations
for _ in {1..300}; do
    sleep 0.1
    if [[ $($CLICKHOUSE_CLIENT --query="SELECT count() FROM system.mutations WHERE database='$CLICKHOUSE_DATABASE' AND table='mt'") -ge 2 ]]; then
        break
    fi
done

tx 10                                            "alter table mt update n=n+20 where 1" | grep -Eo "Deadlock detected" | uniq
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT storage_shared_merge_tree_mutate_pause_before_wait;"
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES mt;"
tx_wait 9
tx 9 "commit"
tx 10                                            "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 10                                            "rollback"

wait_for_mutation "mt" "mutation_3.txt"
$CLICKHOUSE_CLIENT -q "select 'after', n, _part from mt order by n"
$CLICKHOUSE_CLIENT -q "drop table mt"
