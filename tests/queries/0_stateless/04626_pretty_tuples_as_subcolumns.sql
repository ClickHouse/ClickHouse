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

-- Tuple subcolumns preserve the interactive chunk-gluing contract when their layout is unchanged.
SELECT sleep(0.01) AS s, number AS n, (number, toString(number))::Tuple(a UInt64, b String) AS t
FROM numbers(4) SETTINGS max_block_size = 1, output_format_pretty_glue_chunks = 1, output_format_pretty_squash_consecutive_ms = 0 FORMAT PrettyCompact;

-- The footer of a chunk with tuple subcolumns is several lines tall; gluing has to rewind all of them,
-- otherwise a stale footer stays above the rows of the next chunk.
SELECT sleep(0.01) AS s, number AS n, (number, toString(number))::Tuple(a UInt64, b String) AS t
FROM numbers(4) SETTINGS max_block_size = 1, output_format_pretty_glue_chunks = 1, output_format_pretty_squash_consecutive_ms = 0,
    output_format_pretty_display_footer_column_names = 1, output_format_pretty_display_footer_column_names_min_rows = 1 FORMAT PrettyCompact;

SELECT sleep(0.01) AS s, number AS n, (number, toString(number))::Tuple(a UInt64, b String) AS t
FROM numbers(4) SETTINGS max_block_size = 1, output_format_pretty_glue_chunks = 1, output_format_pretty_squash_consecutive_ms = 0,
    output_format_pretty_display_footer_column_names = 1, output_format_pretty_display_footer_column_names_min_rows = 1 FORMAT Pretty;

SELECT sleep(0.01) AS s, number AS n, (number, toString(number))::Tuple(a UInt64, b String) AS t
FROM numbers(4) SETTINGS max_block_size = 1, output_format_pretty_glue_chunks = 1, output_format_pretty_squash_consecutive_ms = 0,
    output_format_pretty_display_footer_column_names = 1, output_format_pretty_display_footer_column_names_min_rows = 1 FORMAT PrettySpace;

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

-- An empty tuple has no subcolumns and keeps the single-cell rendering.
SELECT tuple() AS t FORMAT PrettyCompact;
SELECT tuple() AS t, (1, 2)::Tuple(a UInt8, b UInt8) AS u FORMAT PrettyCompact;

-- A one-element tuple is still a group: the header keeps both the tuple name and the element name,
-- so two one-element tuples with the same element name stay distinguishable.
SELECT (1)::Tuple(a UInt8) AS t, (2)::Tuple(a UInt8) AS u FORMAT PrettyCompact;
SELECT (1)::Tuple(a UInt8) AS t FORMAT Pretty;

-- Nullable tuple: the elements are extracted as Nullable subcolumns with the parent null map applied,
-- so the rows where the whole tuple is NULL display as NULL in every subcolumn.
SET enable_nullable_tuple_type = 1;
SELECT if(number = 1, NULL, (number, toString(number))::Tuple(a UInt64, b String))::Nullable(Tuple(a UInt64, b String)) AS t
FROM numbers(3) FORMAT PrettyCompact;

-- A Nullable tuple with an element that cannot represent NULL keeps the single-cell rendering.
SELECT materialize((1, (2, 3)))::Nullable(Tuple(a UInt8, inner Tuple(m UInt8, n UInt8))) AS t FORMAT PrettyCompact;

-- A long non-identifier tuple name that fits inside the combined width of wide subcolumns, but exceeds
-- output_format_pretty_max_column_pad_width, is rendered exactly as accounted: the header and footer stay aligned.
SELECT (repeat('x', 130), repeat('y', 130))::Tuple(a String, b String) AS `the tuple alias with spaces the tuple alias with spaces the tuple alias with spaces the tuple alias with spaces the tuple alias with spaces the tuple alias with spaces the tuple alias with spaces the tuple alias with spaces the tuple alias with spaces end`
FORMAT PrettyCompact SETTINGS output_format_pretty_display_footer_column_names_min_rows = 1;
