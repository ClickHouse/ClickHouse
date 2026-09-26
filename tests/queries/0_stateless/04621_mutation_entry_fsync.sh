#!/usr/bin/env bash
# Tags: no-object-storage, no-shared-merge-tree, no-parallel
# Tag no-object-storage: object storage disks do not fsync local directories.
# Tag no-shared-merge-tree: the mutation_*.txt entry file is a plain-MergeTree-only durability record;
#   SharedMergeTree keeps mutations in Keeper.
# Tag no-parallel: the rollback case toggles the server-global failpoint mt_throw_after_mutation_commit,
#   which would affect any concurrent plain-MergeTree mutation (same reason as 04308).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/111380
# Both mutation-entry lifecycle transitions on plain MergeTree must fsync the table directory
# when fsync_part_directory = 1, so they survive a power loss (the mutation_*.txt file is the only
# on-disk commit record). The DirectorySync ProfileEvent is the durability proxy (a real power cut
# cannot be simulated in a stateless test). The transition is attributed to the ALTER / KILL query,
# so the count is read from system.query_log.

# Direction 1: the create-commit rename (tmp_mutation_N.txt -> mutation_N.txt).
run_create() {
    local fsync=$1
    $CLICKHOUSE_CLIENT -m -q "
        drop table if exists t_me_c;
        create table t_me_c (id UInt64) engine = MergeTree order by id settings fsync_part_directory = $fsync;
        insert into t_me_c values (1);
    "
    local query_id="me-create-fsync${fsync}-$CLICKHOUSE_DATABASE"
    # throwIf keeps the mutation pending, so its presence is the entry file, not a completed mutation.
    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "alter table t_me_c delete where throwIf(id >= 0) settings mutations_sync = 0"
    $CLICKHOUSE_CLIENT -m --param_query_id "$query_id" -q "
        system flush logs query_log;
        select ProfileEvents['DirectorySync'] >= 1
        from system.query_log
        where event_date >= yesterday() and current_database = currentDatabase()
          and query_id = {query_id:String} and type = 'QueryFinish'
        order by event_time_microseconds desc limit 1;
    "
    # The mutation must be registered regardless of the setting.
    $CLICKHOUSE_CLIENT -q "select count() from system.mutations where database = currentDatabase() and table = 't_me_c'"
    $CLICKHOUSE_CLIENT -q "drop table t_me_c"
}

# Direction 3: the compensating unlink when an ALTER throws after the commit rename (before the
# entry is registered). Once the rename is durable, its rollback unlink must be durable too, or a
# power loss could restore a mutation for an ALTER that returned an error. mt_throw_after_mutation_commit
# fires right after commit, so the entry destructor removes the orphaned file: with fsync on the query
# performs two directory syncs (the commit rename and the orphan unlink).
run_rollback() {
    local fsync=$1
    $CLICKHOUSE_CLIENT -m -q "
        drop table if exists t_me_r;
        create table t_me_r (id UInt64) engine = MergeTree order by id settings fsync_part_directory = $fsync;
        insert into t_me_r values (1);
    "
    local query_id="me-rollback-fsync${fsync}-$CLICKHOUSE_DATABASE"
    $CLICKHOUSE_CLIENT -q "system enable failpoint mt_throw_after_mutation_commit"
    # The ALTER must throw; the orphaned mutation_N.txt is removed by the entry destructor.
    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "alter table t_me_r delete where id >= 0 settings mutations_sync = 0" 2>/dev/null \
        && echo "FAIL: ALTER unexpectedly succeeded; failpoint did not fire"
    $CLICKHOUSE_CLIENT -q "system disable failpoint mt_throw_after_mutation_commit"
    # Expect two directory syncs at fsync=1: the commit rename AND the compensating unlink. Asserting
    # >=2 (not >=1) is what catches a regression in the destructor-side unlink sync, because the commit
    # rename alone already contributes one sync before the failpoint fires.
    $CLICKHOUSE_CLIENT -m --param_query_id "$query_id" -q "
        system flush logs query_log;
        select ProfileEvents['DirectorySync'] >= 2
        from system.query_log
        where event_date >= yesterday() and current_database = currentDatabase()
          and query_id = {query_id:String} and type = 'ExceptionBeforeStart'
        order by event_time_microseconds desc limit 1;
    "
    # No mutation must remain: the rolled-back entry was removed.
    $CLICKHOUSE_CLIENT -q "select count() from system.mutations where database = currentDatabase() and table = 't_me_r'"
    $CLICKHOUSE_CLIENT -q "drop table t_me_r"
}

# Direction 2: the KILL MUTATION unlink of mutation_N.txt.
run_kill() {
    local fsync=$1
    $CLICKHOUSE_CLIENT -m -q "
        drop table if exists t_me_k;
        create table t_me_k (id UInt64) engine = MergeTree order by id settings fsync_part_directory = $fsync;
        insert into t_me_k values (1);
        alter table t_me_k delete where throwIf(id >= 0) settings mutations_sync = 0;
    "
    # Wait until the mutation is registered (and failing) before killing it.
    for _ in {1..50}; do
        [ "$($CLICKHOUSE_CLIENT -q "select count() from system.mutations where database = currentDatabase() and table = 't_me_k'")" = "1" ] && break
        sleep 0.3
    done
    local query_id="me-kill-fsync${fsync}-$CLICKHOUSE_DATABASE"
    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "kill mutation where database = currentDatabase() and table = 't_me_k' sync" > /dev/null
    $CLICKHOUSE_CLIENT -m --param_query_id "$query_id" -q "
        system flush logs query_log;
        select ProfileEvents['DirectorySync'] >= 1
        from system.query_log
        where event_date >= yesterday() and current_database = currentDatabase()
          and query_id = {query_id:String} and type = 'QueryFinish'
        order by event_time_microseconds desc limit 1;
    "
    # The mutation must be gone regardless of the setting.
    $CLICKHOUSE_CLIENT -q "select count() from system.mutations where database = currentDatabase() and table = 't_me_k'"
    $CLICKHOUSE_CLIENT -q "drop table t_me_k"
}

echo "create-commit fsync_part_directory = 1: DirectorySync>=1, mutation registered"
run_create 1
echo "create-commit fsync_part_directory = 0: DirectorySync=0 (setting honored), mutation registered"
run_create 0
echo "kill fsync_part_directory = 1: DirectorySync>=1, mutation removed"
run_kill 1
echo "kill fsync_part_directory = 0: DirectorySync=0 (setting honored), mutation removed"
run_kill 0
echo "rollback fsync_part_directory = 1: DirectorySync>=2 (rename + unlink), no mutation left"
run_rollback 1
echo "rollback fsync_part_directory = 0: DirectorySync=0 (setting honored), no mutation left"
run_rollback 0
