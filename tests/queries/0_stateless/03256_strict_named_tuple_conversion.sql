SET enable_analyzer = 1;
SET enable_named_columns_in_function_tuple = 1;

select (1 as a2, 2 as b2)::Tuple(a int, b int) by_name settings allow_named_tuple_conversion_with_extra_source_fields = 0; -- { serverError CANNOT_CONVERT_TYPE }
select (1 as a2, 2 as b2)::Tuple(a int, b int) by_name settings allow_named_tuple_conversion_with_extra_source_fields = 1;

drop table if exists x;

create table x (t Tuple(a int, b int)) engine MergeTree order by ();

insert into x select (1 as a2, 2 as b2) settings allow_named_tuple_conversion_with_extra_source_fields_on_insert = 0; -- { serverError CANNOT_CONVERT_TYPE }
insert into x select (1 as a2, 2 as b2) settings allow_named_tuple_conversion_with_extra_source_fields_on_insert = 1;

select * from x format JSONEachRow;

drop table x;
