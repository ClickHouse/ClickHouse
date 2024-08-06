#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
drop table if exists forget_partition;

create table forget_partition
(
    k UInt64,
    d Date,
    v String
)
engine = ReplicatedMergeTree('/test/02995/{database}/rmt', '1')
order by (k, d)
partition by toYYYYMMDD(d);

insert into forget_partition select number, '2024-01-01' + interval number day, randomString(20) from system.numbers limit 10;

alter table forget_partition drop partition '20240101';
alter table forget_partition drop partition '20240102';
"""

# DROP PARTITION do not wait for a part to be removed from memory due to possible concurrent SELECTs, so we have to do wait manually here
while [[ $(${CLICKHOUSE_CLIENT} -q "select count() from system.parts where database=currentDatabase() and table='forget_partition' and partition='20240101'") != 0 ]]; do sleep 0.1; done

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
set allow_unrestricted_reads_from_keeper=1;

select '---before---';
select name from system.zookeeper where path = '/test/02995/' || currentDatabase() || '/rmt/block_numbers' order by name;

alter table forget_partition forget partition '20240103'; -- {serverError CANNOT_FORGET_PARTITION}
alter table forget_partition forget partition '20240203'; -- {serverError CANNOT_FORGET_PARTITION}
alter table forget_partition forget partition '20240101';


select '---after---';
select name from system.zookeeper where path = '/test/02995/' || currentDatabase() || '/rmt/block_numbers' order by name;

drop table forget_partition;
"""
