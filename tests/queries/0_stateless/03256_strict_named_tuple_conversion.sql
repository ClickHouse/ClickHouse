SET enable_analyzer = 1;
SET enable_named_columns_in_function_tuple = 1;

select (1 as a2, 2 as b2)::Tuple(a int, b int) by_name settings strict_named_tuple_conversion = 1; -- { serverError CANNOT_CONVERT_TYPE }
select (1 as a2, 2 as b2)::Tuple(a int, b int) by_name settings strict_named_tuple_conversion = 0;
