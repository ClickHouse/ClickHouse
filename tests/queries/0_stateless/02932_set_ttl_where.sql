create or replace table temp (
    a UInt32
)
engine = MergeTree
order by a;

insert into temp select number from system.numbers limit 100_000;

create or replace table t_temp (
    a UInt32,
    timestamp DateTime
)
engine = MergeTree
order by a
TTL timestamp + INTERVAL 2 SECOND WHERE a in (select a from temp);

select sleep(1);
insert into t_temp select rand(), now() from system.numbers limit 1_000_000;
select sleep(1);
insert into t_temp select rand(), now() from system.numbers limit 1_000_000;
select sleep(1);
optimize table t_temp final;
