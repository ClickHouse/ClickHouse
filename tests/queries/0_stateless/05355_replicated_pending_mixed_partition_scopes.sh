#!/usr/bin/env bash
# Tags: zookeeper

# On `ReplicatedMergeTree` the queue keys a mutation entry by the partitions of all of its commands, so
# one `ALTER` with a scoped `CLEAR COLUMN c IN PARTITION '1'` and an unscoped sibling (or another scope)
# put the entry in every partition and used to hand the scoped command to the parts of other partitions
# while the entry was pending. Read on the fly from the replica that did not run the `ALTER`.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The commands of a pending entry are applied on the fly only once the replica has executed the
# `ALTER_METADATA` entry of the `ALTER`. `SYSTEM SYNC REPLICA ... PULL` only puts it into the queue, and a
# full `SYSTEM SYNC REPLICA` would also wait for the `MUTATE_PART` entries, which are assigned even with
# merges stopped and are not executed then. So wait for the `ALTER_METADATA` entry alone.
function wait_for_alter_metadata()
{
    local table=$1
    $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA $table PULL"
    while [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.replication_queue WHERE database = currentDatabase() AND table = '$table' AND type = 'ALTER_METADATA'") != 0 ]]
    do
        sleep 0.1
    done
}

function run_case()
{
    local name=$1
    local title=$2
    local alter=$3

    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS ${name}_r1 SYNC;
        DROP TABLE IF EXISTS ${name}_r2 SYNC;
        CREATE TABLE ${name}_r1 (p UInt8, id UInt64, c UInt64 DEFAULT 777, d UInt64 DEFAULT 888)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$name', 'r1') PARTITION BY p ORDER BY id;
        CREATE TABLE ${name}_r2 (p UInt8, id UInt64, c UInt64 DEFAULT 777, d UInt64 DEFAULT 888)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$name', 'r2') PARTITION BY p ORDER BY id;
        INSERT INTO ${name}_r1 VALUES (1, 1, 10, 100), (2, 2, 20, 200);
        SYSTEM SYNC REPLICA ${name}_r2;

        -- Mutations cannot be executed while merges are stopped, so the entries stay pending.
        SYSTEM STOP MERGES ${name}_r1;
        SYSTEM STOP MERGES ${name}_r2;

        SELECT '$title';
        ALTER TABLE ${name}_r1 $alter SETTINGS alter_sync = 0;
    "

    wait_for_alter_metadata "${name}_r2"

    $CLICKHOUSE_CLIENT --query "
        SELECT p, id, c, d FROM ${name}_r2 ORDER BY id SETTINGS apply_mutations_on_fly = 1;

        SYSTEM START MERGES ${name}_r1;
        SYSTEM START MERGES ${name}_r2;
        ALTER TABLE ${name}_r1 DELETE WHERE 0 SETTINGS mutations_sync = 2;
        SYSTEM SYNC REPLICA ${name}_r2;
        SELECT 'after it materialized';
        SELECT p, id, c, d FROM ${name}_r2 ORDER BY id;

        DROP TABLE ${name}_r1 SYNC;
        DROP TABLE ${name}_r2 SYNC;
    "
}

run_case t_pending_scopes 'a scoped and an unscoped command in one pending entry' \
    "CLEAR COLUMN c IN PARTITION '1', CLEAR COLUMN d"

run_case t_pending_two_scopes 'two scoped commands in one pending entry do not cross-apply' \
    "CLEAR COLUMN c IN PARTITION '1', CLEAR COLUMN d IN PARTITION '2'"
