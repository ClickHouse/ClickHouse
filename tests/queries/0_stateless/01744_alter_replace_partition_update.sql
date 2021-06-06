drop table if exists x;
drop table if exists x1;
drop table if exists x2;

-- replace update partition also works for tables with partition key in primary key, as long as we don't modify primary keys
create table x (type Enum8('hourly' = 0, 'hourly_staging' = 1, 'daily' = 2, 'daily_staging' = 3), dt DateTime, i int)
engine MergeTree partition by (type, if(toUInt8(type) < 2, dt, toStartOfDay(dt))) order by (dt, i);

insert into x values ('daily', '2021-03-28 01:00:00', 10), ('daily', '2021-03-28 02:00:00', 20);
insert into x values ('hourly', '2021-03-28 05:00:00', 60), ('hourly', '2021-03-28 06:00:00', 70);
insert into x values ('hourly', '2021-03-28 07:00:00', 80), ('hourly', '2021-03-28 08:00:00', 90);
insert into x values ('hourly', '2021-03-29 05:00:00', 60), ('hourly', '2021-03-29 06:00:00', 70);

select * from x order by type, dt, i;

-- replace from existing partition to existing partition
alter table x replace partition ('daily', '2021-03-28 00:00:00') from partition ('hourly', '2021-03-28 07:00:00') update type = 'daily';

-- replace from existing partition to non-existing partition
alter table x replace partition ('daily', '2021-03-29 00:00:00') from partition ('hourly', '2021-03-29 06:00:00') update type = 'daily';

-- replace from non-existing partition to non-existing partition
alter table x replace partition ('daily', '2021-03-30 00:00:00') from partition ('hourly', '2021-03-09 06:00:00') update type = 'daily';

-- replace from non-existing partition to existing partition
-- replace will do nothing in this case
alter table x replace partition ('daily', '2021-03-28 00:00:00') from partition ('hourly', '2021-03-09 06:00:00') update type = 'daily';

-- attach partition
alter table x attach partition ('daily', '2021-03-28 00:00:00') from partition ('hourly', '2021-03-28 08:00:00') update type = 'daily';

select * from x order by type, dt, i;

create table x1 (type Enum8('hourly' = 0, 'hourly_staging' = 1, 'daily' = 2, 'daily_staging' = 3), dt DateTime, i int) engine ReplicatedMergeTree('/clickhouse/tables/test_01744/01/x', '1') partition by (type, if(toUInt8(type) < 2, dt, toStartOfDay(dt))) order by (dt, i);

create table x2 (type Enum8('hourly' = 0, 'hourly_staging' = 1, 'daily' = 2, 'daily_staging' = 3), dt DateTime, i int) engine ReplicatedMergeTree('/clickhouse/tables/test_01744/01/x', '2') partition by (type, if(toUInt8(type) < 2, dt, toStartOfDay(dt))) order by (dt, i);

insert into x1 values ('daily', '2021-03-28 01:00:00', 10), ('daily', '2021-03-28 02:00:00', 20);
insert into x1 values ('hourly', '2021-03-28 05:00:00', 60), ('hourly', '2021-03-28 06:00:00', 70);
insert into x1 values ('hourly', '2021-03-28 07:00:00', 80), ('hourly', '2021-03-28 08:00:00', 90);
insert into x1 values ('hourly', '2021-03-29 05:00:00', 60), ('hourly', '2021-03-29 06:00:00', 70);

select * from x1 order by type, dt, i;

-- replace from existing partition to existing partition
alter table x1 replace partition ('daily', '2021-03-28 00:00:00') from partition ('hourly', '2021-03-28 07:00:00') update type = 'daily';

-- replace from existing partition to non-existing partition
alter table x1 replace partition ('daily', '2021-03-29 00:00:00') from partition ('hourly', '2021-03-29 06:00:00') update type = 'daily';

-- replace from non-existing partition to non-existing partition
alter table x1 replace partition ('daily', '2021-03-29 00:00:00') from partition ('hourly', '2021-03-09 06:00:00') update type = 'daily';

-- replace from non-existing partition to existing partition
-- replace will do nothing in this case
alter table x1 replace partition ('daily', '2021-03-28 00:00:00') from partition ('hourly', '2021-03-09 06:00:00') update type = 'daily';

-- attach partition
alter table x1 attach partition tuple('daily', '2021-03-28 00:00:00') from partition ('hourly', '2021-03-28 08:00:00') update type = 'daily';

select * from x1 order by type, dt, i;

system sync replica x2;

select * from x2 order by type, dt, i;

-- error cases

-- mismatched partition
alter table x1 replace partition ('daily', '2021-03-28 00:00:00') from partition ('hourly', '2021-03-28 08:00:00') update type = 'hourly'; -- { serverError 248 }

-- expression generates multiple partitions.
alter table x1 replace partition tuple('daily', '2021-03-28 00:00:00') from partition ('hourly', '2021-03-28 08:00:00') update type = if(rowNumberInBlock() % 2 = 0, 'hourly', 'daily'); -- { serverError 248 }

-- update primary keys
alter table x1 replace partition tuple('daily', '2021-03-28 00:00:00') from partition ('hourly', '2021-03-28 08:00:00') update type = 'daily', dt = '2021-03-28 00:00:00'; -- { serverError 420 }

drop table x;
drop table x1;
drop table x2;
