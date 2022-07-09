-- Tags: long, no-s3-storage, no-backward-compatibility-check

drop table if exists tp_1;
drop table if exists tp_2;

create table tp_1 (x Int32, y Int32, projection p (select x, y order by x)) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/01710_projection_fetch_' || currentDatabase(), '1_{replica}') order by y settings min_rows_for_compact_part = 2, min_rows_for_wide_part = 4, min_bytes_for_compact_part = 16, min_bytes_for_wide_part = 32;

create table tp_2 (x Int32, y Int32, projection p (select x, y order by x)) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/01710_projection_fetch_' || currentDatabase(), '2_{replica}') order by y settings min_rows_for_compact_part = 2, min_rows_for_wide_part = 4, min_bytes_for_compact_part = 16, min_bytes_for_wide_part = 32;

insert into tp_1 select number, number from numbers(3);

system sync replica tp_2;
select * from tp_2 order by x;

insert into tp_1 select number, number from numbers(5);

system sync replica tp_2;
select * from tp_2 order by x;

-- test projection creation, materialization, clear and drop
alter table tp_1 add projection pp (select x, count() group by x);
system sync replica tp_2;
select count() from system.projection_parts where database = currentDatabase() and table = 'tp_2' and name = 'pp' and active;
show create table tp_2;

-- all other three operations are mutations
set mutations_sync = 2;
alter table tp_1 materialize projection pp;
select count() from system.projection_parts where database = currentDatabase() and table = 'tp_2' and name = 'pp' and active;
show create table tp_2;

alter table tp_1 clear projection pp;
system sync replica tp_2;
select * from system.projection_parts where database = currentDatabase() and table = 'tp_2' and name = 'pp' and active;
show create table tp_2;

alter table tp_1 drop projection pp;
system sync replica tp_2;
select * from system.projection_parts where database = currentDatabase() and table = 'tp_2' and name = 'pp' and active;
show create table tp_2;

drop table if exists tp_1;
drop table if exists tp_2;
