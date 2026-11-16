-- Tags: no-fasttest

SET enable_json_type = 1;
set allow_experimental_variant_type = 1;
set use_variant_as_common_type = 1;
set enable_named_columns_in_function_tuple = 1;
set enable_analyzer = 1;

select 'Map to JSON';
select map('a', number::UInt32, 'b', toDate(number), 'c', range(number), 'd', [map('e', number::UInt32)])::JSON as json, JSONAllPathsWithTypes(json) from numbers(5);
select map('a' || number % 3, number::UInt32, 'b' || number % 3, toDate(number), 'c' || number % 3, range(number), 'd' || number % 3, [map('e' || number % 3, number::UInt32)])::JSON as json, JSONAllPathsWithTypes(json) from numbers(5);

select 'Tuple to JSON';
select tuple(number::UInt32 as a, toDate(number) as b, range(number) as c, [tuple(number::UInt32 as e)] as d)::JSON as json, JSONAllPathsWithTypes(json) from numbers(5);
