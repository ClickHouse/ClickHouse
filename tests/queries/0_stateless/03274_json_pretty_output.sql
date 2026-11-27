set enable_named_columns_in_function_tuple=1;
set output_format_write_statistics=0;
set enable_analyzer=1;

select tuple(1 as b, 2 as c) as a, map('e', 3, 'f', 4) as d, [5, 6] as g, tuple(map('i', [tuple(7 as j, 8 as k)]) as l, 42 as m) as h format JSON;
select tuple(1 as b, 2 as c) as a, map('e', 3, 'f', 4) as d, [5, 6] as g, tuple(map('i', [tuple(7 as j, 8 as k)]) as l, 42 as m) as h format JSON settings output_format_json_pretty_print=0;

