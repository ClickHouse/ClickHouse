-- Tags: zookeeper

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

insert into forget_partition select number, '2024-01-01' + interval number day, base64Encode(randomString(20)) from system.numbers limit 10;

alter table forget_partition drop partition '20240101';
alter table forget_partition drop partition '20240102';

set allow_unrestricted_reads_from_keeper=1;

select '---before---';
select name from system.zookeeper where path = '/test/02995/' || currentDatabase() || '/rmt/block_numbers' order by name;

alter table forget_partition forget partition '20240103'; -- {serverError CANNOT_FORGET_PARTITION}
alter table forget_partition forget partition '20240203'; -- {serverError KEEPER_EXCEPTION}
alter table forget_partition forget partition '20240101';


select '---after---';
select name from system.zookeeper where path = '/test/02995/' || currentDatabase() || '/rmt/block_numbers' order by name;

drop table forget_partition;