-- Tags: global

drop table if exists xp;
drop table if exists xp_d;

create table xp(i Nullable(UInt64), j UInt64) engine MergeTree order by i settings index_granularity = 1, allow_nullable_key = 1;
create table xp_d as xp engine Distributed(test_shard_localhost, currentDatabase(), xp);

insert into xp select number, number + 2 from numbers(10);
insert into xp select null, 100;

optimize table xp final;

set max_rows_to_read = 2;
select * from xp where i in [0, 1];
select * from xp where i global in [0, 1];
select * from xp_d where i in [0, 1];
select * from xp_d where i global in [0, 1];

set max_rows_to_read = 4; -- 2 in the subquery, 2 in the query itself
select * from xp where i in (select * from numbers(2));
select * from xp where i global in (select * from numbers(2));
select * from xp_d where i in (select * from numbers(2));

set max_rows_to_read = 6; -- 2 subquery, 2 from global temp table (GLOBAL IN), 2 from local xp table
select * from xp_d where i global in (select * from numbers(2));

set transform_null_in = 1;
set max_rows_to_read = 4; -- 2 in the subquery, 2 in the query itself
select * from xp where i in (select * from numbers(2));
select * from xp where i global in (select * from numbers(2));
select * from xp_d where i in (select * from numbers(2));

set max_rows_to_read = 6; -- 2 subquery, 2 from global temp table (GLOBAL IN), 2 from local xp table
select * from xp_d where i global in (select * from numbers(2));

set max_rows_to_read = 0; -- No rows should be read
select * from xp where i in (null);
select * from xp where i global in (null);
select * from xp_d where i in (null);
select * from xp_d where i global in (null);

drop table if exists xp;
drop table if exists xp_d;
