drop table if exists tp_1;
drop table if exists tp_2;

create table tp_1 (x Int32, y Int32, projection p (select x, y order by x)) engine = ReplicatedMergeTree('/clickhouse/tables/01710_projection_fetch_' || currentDatabase(), '1') order by y settings min_rows_for_compact_part = 2, min_rows_for_wide_part = 4, min_bytes_for_compact_part = 16, min_bytes_for_wide_part = 32;

create table tp_2 (x Int32, y Int32, projection p (select x, y order by x)) engine = ReplicatedMergeTree('/clickhouse/tables/01710_projection_fetch_' || currentDatabase(), '2') order by y settings min_rows_for_compact_part = 2, min_rows_for_wide_part = 4, min_bytes_for_compact_part = 16, min_bytes_for_wide_part = 32;

insert into tp_1 select number, number from numbers(3);

system sync replica tp_2;
select * from tp_2 order by x;

insert into tp_1 select number, number from numbers(5);

system sync replica tp_2;
select * from tp_2 order by x;

drop table if exists tp_1;
drop table if exists tp_2;

