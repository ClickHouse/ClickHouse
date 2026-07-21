-- Tests for output_format_pretty_display_tuples_as_subcolumns.
-- When enabled, tuples in Pretty formats are shown as groups of subcolumns,
-- with the tuple element names in a second header row. Closes #65388.

SET output_format_pretty_display_tuples_as_subcolumns = 1;

-- A named tuple next to a scalar column, in every Pretty variant.
SELECT 'hello' AS x, (1, 'world')::Tuple(a UInt8, b String) AS t FORMAT PrettyCompact;
SELECT 'hello' AS x, (1, 'world')::Tuple(a UInt8, b String) AS t FORMAT Pretty;
SELECT 'hello' AS x, (1, 'world')::Tuple(a UInt8, b String) AS t FORMAT PrettySpace;
-- The Vertical format is unaffected.
SELECT 'hello' AS x, (1, 'world')::Tuple(a UInt8, b String) AS t FORMAT Vertical;

-- Several rows exercise the row separators.
SELECT number AS n, (number, toString(number))::Tuple(a UInt64, b String) AS t FROM numbers(3) FORMAT PrettyCompact;

-- Unnamed tuple: the subcolumns are named by position.
SELECT (1, 'a', 2.5) AS t FORMAT PrettyCompact;

-- The tuple name is wider than its subcolumns combined, so they are widened to fit.
SELECT (1, 2)::Tuple(a UInt8, b UInt8) AS `a rather long tuple column name` FORMAT PrettyCompact;

-- Numeric subcolumns are right-aligned.
SELECT (123456, -7)::Tuple(x Int64, y Int32) AS t FORMAT PrettyCompact;

-- A table without tuples keeps the single-row header even with the setting on.
SELECT 1 AS a, 'x' AS b FORMAT PrettyCompact;

-- Nested named tuple: the inner tuple is rendered as JSON inside its subcolumn.
SELECT (1, (2, 3)::Tuple(m UInt8, n UInt8))::Tuple(a UInt8, inner Tuple(m UInt8, n UInt8)) AS t FORMAT PrettyCompact;

-- The footer column names row is also split into subcolumns.
SELECT (number, toString(number))::Tuple(a UInt64, b String) AS t FROM numbers(2)
FORMAT PrettyCompact SETTINGS output_format_pretty_display_footer_column_names_min_rows = 1;

-- The setting is opt-in: with it off, named tuples are shown as JSON (the default).
SELECT (1, 'world')::Tuple(a UInt8, b String) AS t FORMAT PrettyCompact SETTINGS output_format_pretty_display_tuples_as_subcolumns = 0;
