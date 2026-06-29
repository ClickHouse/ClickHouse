drop table if exists x1;

create table x1 (i Nullable(int)) engine MergeTree order by i desc primary key i settings allow_nullable_key = 1, index_granularity = 2, allow_experimental_reverse_key = 1;

insert into x1 select * from numbers(100);

optimize table x1 final;

select * from x1 where i = 3;

select count() from x1 where i between 3 and 10;

drop table x1;
