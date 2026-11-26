SET output_format_pretty_color = 0;
SET output_format_pretty_named_tuples_as_subcolumns = 1;

SELECT 'Nullable named Tuple keeps the single-cell rendering';
SET enable_nullable_tuple_type = 1;
SET output_format_pretty_named_tuples_as_json = 0;
SELECT number AS x, if(number = 1, NULL, (number, 'str')::Tuple(u UInt64, s String))::Nullable(Tuple(u UInt64, s String)) AS t FROM numbers(3) FORMAT Pretty;
SELECT number AS x, if(number = 1, NULL, (number, 'str')::Tuple(u UInt64, s String))::Nullable(Tuple(u UInt64, s String)) AS t FROM numbers(3) FORMAT PrettyCompact;
SELECT number AS x, if(number = 1, NULL, (number, 'str')::Tuple(u UInt64, s String))::Nullable(Tuple(u UInt64, s String)) AS t FROM numbers(3) FORMAT PrettySpace;

SELECT 'A bare named Tuple next to a Nullable one is still split';
SELECT (1, 'a')::Tuple(u UInt8, s String) AS bare, NULL::Nullable(Tuple(u UInt8, s String)) AS wrapped FORMAT Pretty;

SELECT 'The single-value width exemption follows the logical block shape';
SET output_format_pretty_max_value_width = 10;
SET output_format_pretty_max_value_width_apply_for_single_value = 0;
SELECT (repeat('x', 30), 1)::Tuple(a String, b UInt8) AS t FORMAT Pretty;
SELECT (repeat('x', 30), 1)::Tuple(a String, b UInt8) AS t FORMAT PrettyCompact;

SELECT 'Two logical columns: long values are cut';
SELECT (repeat('x', 30), 1)::Tuple(a String, b UInt8) AS t, 42 AS y FORMAT Pretty;

SELECT 'The exemption can be disabled';
SET output_format_pretty_max_value_width_apply_for_single_value = 1;
SELECT (repeat('x', 30), 1)::Tuple(a String, b UInt8) AS t FORMAT Pretty;
