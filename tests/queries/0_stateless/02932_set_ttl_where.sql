DROP TABLE IF EXISTS t_temp;
create table t_temp (
    a UInt32,
    timestamp DateTime
)
engine = MergeTree
order by a
TTL timestamp + INTERVAL 2 SECOND WHERE a in (select number from system.numbers limit 10_000);

select sleep(1);
insert into t_temp select rand(), now() from system.numbers limit 100_000;
select sleep(1);
insert into t_temp select rand(), now() from system.numbers limit 100_000;
select sleep(1);
optimize table t_temp final;

DROP TABLE t_temp;
