set enable_named_columns_in_function_tuple = 1;
set enable_analyzer = 1;

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

drop table if exists tbl;

-- Make sure named tuple won't break Values insert
create table tbl (x Tuple(a Int32, b Int32, c Int32)) engine MergeTree order by ();
insert into tbl values (tuple(1, 2, 3)); -- without tuple it's interpreted differently inside values block.
select * from tbl;

drop table tbl;

-- Avoid generating named tuple for special keywords
select toTypeName(tuple(null)), toTypeName(tuple(true)), toTypeName(tuple(false));
