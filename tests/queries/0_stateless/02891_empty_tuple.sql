drop table if exists x;

create table x engine MergeTree order by () settings optimize_row_order_if_no_order_by = 0 as select () as a, () as b;

insert into x values ((), ());

select count() from x;

select * from x order by ();

select ();

drop table if exists x;

SET allow_experimental_nullable_tuple_type = 0;

create table x (i Nullable(Tuple())) engine MergeTree order by () settings optimize_row_order_if_no_order_by = 0; -- { serverError ILLEGAL_COLUMN }

SET allow_experimental_nullable_tuple_type = 1;

create table x (i Nullable(Tuple())) engine MergeTree order by () settings optimize_row_order_if_no_order_by = 0;

SET allow_experimental_nullable_tuple_type = DEFAULT;

drop table x;

create table x (i LowCardinality(Tuple())) engine MergeTree order by () settings optimize_row_order_if_no_order_by = 0; -- { serverError 43 }
create table x (i Tuple(), j Array(Tuple())) engine MergeTree order by () settings optimize_row_order_if_no_order_by = 0;

insert into x values ((), [(), ()]), ((), []);

select count() from x;

select * from x order by () settings max_threads = 1;

drop table x;
