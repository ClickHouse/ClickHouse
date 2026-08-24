#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-fasttest
# no-fasttest: Slow wait

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --multiline -q """
drop table if exists forget_partition;

create table forget_partition
(
    k UInt64,
    d Date,
    v String
)
engine = ReplicatedMergeTree('/test/02995/{database}/rmt', '1')
order by (k, d)
partition by toYYYYMMDD(d)
-- Reduce max_merge_selecting_sleep_ms and max_cleanup_delay_period to speed up the part being dropped from memory (RMT)
-- Same with old_parts_lifetime for SMT
SETTINGS old_parts_lifetime=5, merge_selecting_sleep_ms=1000, max_merge_selecting_sleep_ms=5000, cleanup_delay_period=3, max_cleanup_delay_period=5;

insert into forget_partition select number, '2024-01-01' + interval number day, randomString(20) from system.numbers limit 10;

alter table forget_partition drop partition '20240101';
alter table forget_partition drop partition '20240102';
"""

# DROP PARTITION do not wait for a part to be removed from memory due to possible concurrent SELECTs, so we have to do wait manually here
while [[ $(${CLICKHOUSE_CLIENT} -q "select count() from system.parts where database=currentDatabase() and table='forget_partition' and partition IN ('20240101', '20240102')") != 0 ]]; do sleep 1; done

${CLICKHOUSE_CLIENT} --multiline -q """
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
