SET enable_named_columns_in_function_tuple = 1, output_format_pretty_max_column_name_width_cut_to = 5;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT Pretty;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT Pretty;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT Pretty;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT Pretty;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT PrettyCompact;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT PrettyCompact;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT PrettyCompact;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT PrettyCompact;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT PrettySpace;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT PrettySpace;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT PrettySpace;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT PrettySpace;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT Vertical;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT Vertical;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT Vertical;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT Vertical;

SELECT 'hello' AS x, ((1 AS b, 2 AS c) AS a, 'world' AS d) AS t FORMAT Pretty;
SELECT 'hello' AS x, ((1 AS b, (2 AS d, 3 AS e) AS c) AS a, 'world' AS f) AS t FORMAT Pretty;
SELECT (('123456789' AS "kkkkkkkkkkkkkkkkkkkk", 'havegreatday' AS "sdfsdfsdfs") AS "fddddasadfasd", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT Pretty;

SELECT 'hello' AS x, ((1 AS a, ((1 as f, 2 as u) as c, ('dgdsfgdsfgdsfworldfgdfgdsfd' as fgsdfsdf, ('dddsfas' as dfasfsdfsadf, 'dsddd' as hj) as gg)) as d) AS b) AS t FORMAT Pretty;
SELECT 'hello' AS x, (1 AS a, ((1 as f, 2 as u) as c, 'dgdsfgdsfgdsfworldfgdfgdsfd' as d) AS b) AS t FORMAT Pretty;

SET output_format_pretty_max_column_name_width_cut_to = 0;
SELECT 1 AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT PrettyCompact;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT Pretty;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT Pretty;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT Pretty;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT Pretty;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT PrettyCompact;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT PrettyCompact;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT PrettyCompact;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT PrettyCompact;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT PrettySpace;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT PrettySpace;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT PrettySpace;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT PrettySpace;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT Vertical;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT Vertical;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT Vertical;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT Vertical;

SET output_format_pretty_display_footer_column_names_min_rows = 0, output_format_pretty_display_footer_column_names = 1;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT Pretty;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT Pretty;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT Pretty;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT Pretty;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT PrettyCompact;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT PrettyCompact;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT PrettyCompact;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT PrettyCompact;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT PrettySpace;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT PrettySpace;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT PrettySpace;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT PrettySpace;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT Vertical;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT Vertical;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT Vertical;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT Vertical;

SELECT 'hello' AS x, ((1 AS b, 2 AS c) AS a, 'world' AS d) AS t FORMAT Pretty;
SELECT 'hello' AS x, ((1 AS b, (2 AS d, 3 AS e) AS c) AS a, 'world' AS f) AS t FORMAT Pretty;
SELECT (('123456789' AS "kkkkkkkkkkkkkkkkkkkk", 'havegreatday' AS "sdfsdfsdfs") AS "fddddasadfasd", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT Pretty;

SELECT 'hello' AS x, ((1 AS a, ((1 as f, 2 as u) as c, ('dgdsfgdsfgdsfworldfgdfgdsfd' as fgsdfsdf, ('dddsfas' as dfasfsdfsadf, 'dsddd' as hj) as gg)) as d) AS b) AS t FORMAT Pretty;
SELECT 'hello' AS x, (1 AS a, ((1 as f, 2 as u) as c, 'dgdsfgdsfgdsfworldfgdfgdsfd' as d) AS b) AS t FORMAT Pretty;

SELECT 1 AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT PrettyCompact;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT Pretty;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT Pretty;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT Pretty;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT Pretty;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT PrettyCompact;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT PrettyCompact;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT PrettyCompact;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT PrettyCompact;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT PrettySpace;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT PrettySpace;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT PrettySpace;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT PrettySpace;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT Vertical;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT Vertical;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT Vertical;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT Vertical;

SET output_format_pretty_display_tuple_as_subcolumns = 0;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT Pretty;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT Pretty;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT Pretty;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT Pretty;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT PrettyCompact;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT PrettyCompact;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT PrettyCompact;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT PrettyCompact;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT PrettySpace;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT PrettySpace;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT PrettySpace;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT PrettySpace;

SELECT CAST((1, 'Hello') AS Tuple(a UInt64, b String)) AS "абвгдежзийклмнопрстуф", 'Hello' AS x, 'World' AS "абвгдежзийклмнопрстуфхцчшщъыьэюя" FORMAT Vertical;
SELECT 'hello' AS x, (1 AS a, 'world' AS b) AS t FORMAT Vertical;
SELECT ('123456789' AS "kkkkkkkkkkkkkkkkkkkk", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT Vertical;
SELECT (123456789 AS kkkkkkkkkkkkkkkkkkkk, '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b), 'ititititititititititititititit' AS "u5u5u5u5u5u5u5u" FORMAT Vertical;

SELECT 'hello' AS x, ((1 AS b, 2 AS c) AS a, 'world' AS d) AS t FORMAT Pretty;
SELECT 'hello' AS x, ((1 AS b, (2 AS d, 3 AS e) AS c) AS a, 'world' AS f) AS t FORMAT Pretty;
SELECT (('123456789' AS "kkkkkkkkkkkkkkkkkkkk", 'havegreatday' AS "sdfsdfsdfs") AS "fddddasadfasd", '123456789' AS x) AS kkrt, ('abcdefghhhhhiii' AS a, 'llooppaptapt' AS b) AS t FORMAT Pretty;

SELECT 'hello' AS x, ((1 AS a, ((1 as f, 2 as u) as c, ('dgdsfgdsfgdsfworldfgdfgdsfd' as fgsdfsdf, ('dddsfas' as dfasfsdfsadf, 'dsddd' as hj) as gg)) as d) AS b) AS t FORMAT Pretty;
SELECT 'hello' AS x, (1 AS a, ((1 as f, 2 as u) as c, 'dgdsfgdsfgdsfworldfgdfgdsfd' as d) AS b) AS t FORMAT Pretty;