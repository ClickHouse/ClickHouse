-- A single value is not cut when `output_format_pretty_max_value_width_apply_for_single_value` is disabled,
-- and the grid has to grow with it instead of the value spilling past the border.

SET output_format_pretty_color = 0;
SET output_format_pretty_max_value_width = 10;
SET output_format_pretty_max_value_width_apply_for_single_value = 0;

SELECT 'A scalar single value';
SELECT repeat('x', 30) AS a FORMAT Pretty;
SELECT repeat('x', 30) AS a FORMAT PrettyCompact;
SELECT repeat('x', 30) AS a FORMAT PrettySpace;

SELECT 'A lone named Tuple displayed as subcolumns';
SET output_format_pretty_named_tuples_as_subcolumns = 1;
SELECT (repeat('x', 30), 1)::Tuple(a String, b UInt8) AS t FORMAT Pretty;
SELECT (repeat('x', 30), 1)::Tuple(a String, b UInt8) AS t FORMAT PrettyCompact;

SELECT 'Zero means no limit at all';
SET output_format_pretty_max_value_width = 0;
SET output_format_pretty_max_value_width_apply_for_single_value = 1;
SELECT repeat('x', 30) AS a, 1 AS b FROM numbers(2) FORMAT PrettyCompact;
