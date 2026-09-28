-- The visible width of a value containing a tab depends on the position where the cell starts,
-- so the width calculation and the rendering have to agree on it, also after the subcolumns of a
-- named Tuple have been widened to fit the name of the Tuple.

SET output_format_pretty_color = 0;
SET output_format_pretty_max_column_name_width_cut_to = 0;

SELECT 'Plain columns';
SELECT repeat('y', 11) AS x, '\t' AS s, 5 AS z FORMAT PrettyCompact;
SELECT repeat('y', 11) AS x, '\t' AS s, 5 AS z FORMAT Pretty;

SELECT 'A long Tuple name widens its subcolumns';
SET output_format_pretty_named_tuples_as_subcolumns = 1;
SELECT (1, 2)::Tuple(a UInt8, b UInt8) AS this_is_a_rather_long_column_name, '\t' AS s FORMAT PrettyCompact;
SELECT (1, 2)::Tuple(a UInt8, b UInt8) AS this_is_a_rather_long_column_name, '\t' AS s FORMAT Pretty;
