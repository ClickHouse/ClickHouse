drop table if exists mt_compact;

create table mt_compact (a Int, s String) engine = MergeTree order by a partition by a
settings index_granularity_bytes = 0;
alter table mt_compact modify setting min_rows_for_wide_part = 1000; -- { serverError 48 }

create table mt_compact_2 (a Int, s String) engine = MergeTree order by a partition by a
settings min_rows_for_wide_part = 1000;
insert into mt_compact_2 values (1, 'a');
alter table mt_compact attach partition 1 from mt_compact_2; -- { serverError 36 }
