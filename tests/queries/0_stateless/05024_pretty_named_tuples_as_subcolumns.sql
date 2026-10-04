SET output_format_pretty_color = 0;

SELECT 'Basic';
SELECT 'hello' AS x, (1, 'world')::Tuple(a UInt8, b String) AS t FORMAT Pretty;
SELECT 'hello' AS x, (1, 'world')::Tuple(a UInt8, b String) AS t FORMAT PrettyCompact;
SELECT 'hello' AS x, (1, 'world')::Tuple(a UInt8, b String) AS t FORMAT PrettySpace;

SELECT 'Numbers are right-aligned';
SELECT (12345, 'abc')::Tuple(number UInt32, s String) AS t, 42 AS n FORMAT Pretty;

SELECT 'Nested tuples';
SELECT 'hello' AS x, (1, ((2, 3), 'world'))::Tuple(a UInt8, f Tuple(b Tuple(c UInt8, d UInt8), e String)) AS t FORMAT Pretty;
SELECT 'hello' AS x, (1, ((2, 3), 'world'))::Tuple(a UInt8, f Tuple(b Tuple(c UInt8, d UInt8), e String)) AS t FORMAT PrettyCompact;
SELECT 'hello' AS x, (1, ((2, 3), 'world'))::Tuple(a UInt8, f Tuple(b Tuple(c UInt8, d UInt8), e String)) AS t FORMAT PrettySpace;

SELECT 'Footer';
SET output_format_pretty_display_footer_column_names = 1, output_format_pretty_display_footer_column_names_min_rows = 0;
SELECT 'hello' AS x, (1, 'world')::Tuple(a UInt8, b String) AS t FORMAT Pretty;
SELECT 'hello' AS x, (1, 'world')::Tuple(a UInt8, b String) AS t FORMAT PrettyCompact;
SELECT 'hello' AS x, (1, 'world')::Tuple(a UInt8, b String) AS t FORMAT PrettySpace;
SELECT 'hello' AS x, (1, ((2, 3), 'world'))::Tuple(a UInt8, f Tuple(b Tuple(c UInt8, d UInt8), e String)) AS t FORMAT Pretty;
SELECT 'hello' AS x, (1, ((2, 3), 'world'))::Tuple(a UInt8, f Tuple(b Tuple(c UInt8, d UInt8), e String)) AS t FORMAT PrettyCompact;
SELECT 'hello' AS x, (1, ((2, 3), 'world'))::Tuple(a UInt8, f Tuple(b Tuple(c UInt8, d UInt8), e String)) AS t FORMAT PrettySpace;
SET output_format_pretty_display_footer_column_names_min_rows = DEFAULT;

SELECT 'A long Tuple name widens its subcolumns';
SELECT (1, 2)::Tuple(a UInt8, b UInt8) AS this_is_a_rather_long_column_name FORMAT Pretty;
SELECT (1, 2)::Tuple(a UInt8, b UInt8) AS this_is_a_rather_long_column_name SETTINGS output_format_pretty_max_column_name_width_cut_to = 0 FORMAT Pretty;

SELECT 'Multiple rows';
SELECT number AS n, (number * 2, toString(number))::Tuple(twice UInt64, s String) AS t FROM numbers(3) FORMAT Pretty;
SELECT number AS n, (number * 2, toString(number))::Tuple(twice UInt64, s String) AS t FROM numbers(3) FORMAT PrettyCompact;
SELECT number AS n, (number * 2, toString(number))::Tuple(twice UInt64, s String) AS t FROM numbers(3) FORMAT PrettySpace;

SELECT 'Cut in the middle';
SELECT (number, number * 2)::Tuple(a UInt64, b UInt64) AS t FROM numbers(6) SETTINGS output_format_pretty_max_rows = 4 FORMAT PrettyCompact;

SELECT 'Totals';
SELECT number % 2 AS k, (sum(number), count())::Tuple(s UInt64, c UInt64) AS t FROM numbers(10) GROUP BY k WITH TOTALS ORDER BY k FORMAT PrettyCompact;

SELECT 'ASCII grid';
SET output_format_pretty_grid_charset = 'ASCII';
SELECT 'hello' AS x, (1, ((2, 3), 'world'))::Tuple(a UInt8, f Tuple(b Tuple(c UInt8, d UInt8), e String)) AS t FORMAT Pretty;
SELECT 'hello' AS x, (1, 'world')::Tuple(a UInt8, b String) AS t FORMAT PrettyCompact;
SET output_format_pretty_grid_charset = DEFAULT;

SELECT 'Single-element tuple';
SELECT CAST(tuple(42), 'Tuple(a UInt64)') AS t, 'x' AS y FORMAT Pretty;

SELECT 'Unnamed tuples are not split';
SELECT (1, 'world') AS t FORMAT Pretty;

SELECT 'Named tuples nested inside other types are not split';
SELECT [CAST((1, 'a'), 'Tuple(x UInt8, y String)')] AS arr FORMAT PrettyCompact;

SELECT 'Disabled: JSON rendering inside the cell';
SELECT (1, 'world')::Tuple(a UInt8, b String) AS t SETTINGS output_format_pretty_named_tuples_as_subcolumns = 0 FORMAT Pretty;

SELECT 'Both settings disabled: plain rendering inside the cell';
SELECT (1, 'world')::Tuple(a UInt8, b String) AS t SETTINGS output_format_pretty_named_tuples_as_subcolumns = 0, output_format_pretty_named_tuples_as_json = 0 FORMAT Pretty;
