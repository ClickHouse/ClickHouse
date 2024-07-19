-- Test for least/greatest function: if the input has null values, should not return null
drop table if exists tab_base_case;
drop table if exists tab_decimal;

create table tab_base_case(id UInt32, x1 Nullable(Float64), x2 Nullable(Int32)) Engine=MergeTree order by id;
create table tab_decimal(id UInt32, x1 Nullable(Decimal32(5)), x2 Nullable(Decimal64(5))) Engine=MergeTree order by id;

insert into tab_base_case values(1, 2, 3), (2, 3, NULL), (3, NULL, NULL);
insert into tab_decimal   values(1, 2, 3), (2, 3, NULL), (3, NULL, NULL);

select greatest(x1, x2), greatest(x1, x2, NULL), greatest(x1, x2, 4), greatest(x1, NULL, 4), greatest(x1, NULL, NULL), greatest(NULL, NULL, NULL) from tab_base_case;
select least(x1, x2),    least(x1, x2, NULL),    least(x1, x2, 4),    least(x1, NULL, 4),    least(x1, NULL, NULL),    least(NULL, NULL, NULL)    from tab_base_case;

select greatest(x1, x2), greatest(x1, x2, NULL), greatest(x1, x2, 4), greatest(x1, NULL, 4), greatest(x1, NULL, NULL), greatest(NULL, NULL, NULL) from tab_decimal;
select least(x1, x2),    least(x1, x2, NULL),    least(x1, x2, 4),    least(x1, NULL, 4),    least(x1, NULL, NULL),    least(NULL, NULL, NULL)    from tab_decimal;
select greatest(1, 2, NULL),  least(1, 2, NULL);

drop table tab_base_case;
drop table tab_decimal;