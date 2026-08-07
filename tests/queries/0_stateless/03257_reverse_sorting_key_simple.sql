-- Tags: no-random-merge-tree-settings

set optimize_read_in_order = 1;
set read_in_order_two_level_merge_threshold=100;

drop table if exists x1;

drop table if exists x2;

create table x1 (i Nullable(int)) engine MergeTree order by i desc settings allow_nullable_key = 1, index_granularity = 2, allow_experimental_reverse_key = 1;

insert into x1 select * from numbers(100);

optimize table x1 final;

select * from x1 where i = 3;

select count() from x1 where i between 3 and 10;

select * from x1 order by i desc limit 5;

select * from x1 order by i limit 5;

create table x2 (i Nullable(int), j Nullable(int)) engine MergeTree order by (i, j desc) settings allow_nullable_key = 1, index_granularity = 2, allow_experimental_reverse_key = 1;

insert into x2 select number % 10, number + 1000 from numbers(100);

optimize table x2 final;

select * from x2 where j = 1003;

select count() from x2 where i between 3 and 10 and j between 1003 and 1008;

select * from x2 order by i, j desc limit 5;

select * from x2 order by i, j limit 5;

drop table x1;

drop table x2;
