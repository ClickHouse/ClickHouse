drop table if exists cc sync;
create table cc (a UInt64, b String) ENGINE = MergeTree order by (a, b) SETTINGS compress_marks = true;
insert into cc  values (2, 'World');
alter table cc detach part 'all_1_1_0';
alter table cc attach part 'all_1_1_0';
select * from cc;
