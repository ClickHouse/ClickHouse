drop table if exists xp;
drop table if exists xp_d;

create table xp(i UInt64, j UInt64) engine MergeTree order by i settings index_granularity = 1;
create table xp_d as xp engine Distributed(test_shard_localhost, currentDatabase(), xp);

insert into xp select number, number + 2 from numbers(10);

set max_rows_to_read = 2;
select * from xp where i in (select * from numbers(2));
select * from xp where i global in (select * from numbers(2));
select * from xp_d where i in (select * from numbers(2));
select * from xp_d where i global in (select * from numbers(2));

drop table if exists xp;
drop table if exists xp_d;
