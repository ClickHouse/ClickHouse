#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree, no-fasttest
# SMT: The merge process is completely different from RMT
# no-fasttest: Avoid long waits

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

set -e

function wait_until()
{
    local q=$1 && shift
    while [ "$($CLICKHOUSE_CLIENT -m -q "$q")" != "1" ]; do
        sleep 0.5
    done
}

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists rmt_master;
    drop table if exists rmt_slave;

    create table rmt_master (key Int) engine=ReplicatedMergeTree('/clickhouse/{database}', 'master') order by tuple() settings always_fetch_merged_part=0, old_parts_lifetime=600;
    -- prefer_fetch_merged_part_*_threshold=0, consider this table as a 'slave'
    create table rmt_slave (key Int) engine=ReplicatedMergeTree('/clickhouse/{database}', 'slave') order by tuple() settings prefer_fetch_merged_part_time_threshold=0, prefer_fetch_merged_part_size_threshold=0, old_parts_lifetime=600;

    insert into rmt_master values (1);

    system sync replica rmt_master;
    system sync replica rmt_slave;
    system stop replicated sends rmt_master;
    system stop pulling replication log rmt_slave;
    alter table rmt_master update key=key+100 where 1 settings alter_sync=1;
"

# first we need to make the rmt_master execute mutation so that it will have
# the part, and rmt_slave will consider it instead of performing mutation on
# it's own, otherwise prefer_fetch_merged_part_*_threshold will be simply ignored
wait_for_mutation rmt_master 0000000000
$CLICKHOUSE_CLIENT -m -q "system start pulling replication log rmt_slave"
# and wait until rmt_slave to fetch the part and reflect this error in system.part_log
wait_until "select count()>0 from system.part_log where table = 'rmt_slave' and database = '$CLICKHOUSE_DATABASE' and error > 0"
$CLICKHOUSE_CLIENT -m -q "
    select 'before';
    select table, event_type, error>0, countIf(error=0) from system.part_log where database = currentDatabase() group by 1, 2, 3 order by 1, 2, 3;

    system start replicated sends rmt_master;
"
wait_for_mutation rmt_slave 0000000000
$CLICKHOUSE_CLIENT -m -q "
    system sync replica rmt_slave;

    system flush logs;
    select 'after';
    select table, event_type, error>0, countIf(error=0) from system.part_log where database = currentDatabase() group by 1, 2, 3 order by 1, 2, 3;

    drop table rmt_master;
    drop table rmt_slave;
"
