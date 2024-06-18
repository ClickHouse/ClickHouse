set enable_named_columns_in_function_tuple = 1;
set allow_experimental_analyzer = 1;

drop table if exists x;
create table x (i int, j int) engine MergeTree order by i;
insert into x values (1, 2);

select toTypeName(tuple(i, j)) from x;
select tupleNames(tuple(i, j)) from x;

select toTypeName(tuple(1, j)) from x;
select tupleNames(tuple(1, j)) from x;

select toTypeName(tuple(1 as k, j)) from x;
select tupleNames(tuple(1 as k, j)) from x;

select toTypeName(tuple(i, i, j, j)) from x;
select tupleNames(tuple(i, i, j, j)) from x;

select tupleNames(1); -- { serverError 43 }

drop table x;
