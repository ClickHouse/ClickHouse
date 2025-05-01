SET output_format_write_statistics = 0;
SET enable_named_columns_in_function_tuple = 0;

SELECT number * 246 + 10 AS n, toDate('2000-01-01') + n AS d, range(n) AS arr, arrayStringConcat(arrayMap(x -> reinterpretAsString(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT RowBinary;
SELECT number * 246 + 10 AS n, toDate('2000-01-01') + n AS d, range(n) AS arr, arrayStringConcat(arrayMap(x -> reinterpretAsString(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT RowBinaryWithNamesAndTypes;
SELECT number * 246 + 10 AS n, toDate('2000-01-01') + n AS d, range(n) AS arr, arrayStringConcat(arrayMap(x -> reinterpretAsString(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT TabSeparatedWithNamesAndTypes;
SELECT number * 246 + 10 AS n, toDate('2000-01-01') + n AS d, range(n) AS arr, arrayStringConcat(arrayMap(x -> reinterpretAsString(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT TabSeparatedRaw;
SELECT number * 246 + 10 AS n, toDate('2000-01-01') + n AS d, range(n) AS arr, arrayStringConcat(arrayMap(x -> reinterpretAsString(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT CSV;
SELECT number * 246 + 10 AS n, toDate('2000-01-01') + n AS d, range(n) AS arr, arrayStringConcat(arrayMap(x -> reinterpretAsString(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT JSON;
SELECT number * 246 + 10 AS n, toDate('2000-01-01') + n AS d, range(n) AS arr, arrayStringConcat(arrayMap(x -> reinterpretAsString(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT JSONCompact;
SELECT number * 246 + 10 AS n, toDate('2000-01-01') + n AS d, range(n) AS arr, arrayStringConcat(arrayMap(x -> reinterpretAsString(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT XML;

SET enable_named_columns_in_function_tuple = 1;

SELECT 36 AS n, toDate('2000-01-01') + n AS d, (n, d) AS tuple FROM system.numbers LIMIT 1 FORMAT RowBinaryWithNamesAndTypes SETTINGS allow_experimental_analyzer=1;
SELECT number * 246 + 10 AS n, toDate('2000-01-01') + n AS d, (n, d) AS tuple FROM system.numbers LIMIT 1 FORMAT TabSeparatedWithNamesAndTypes SETTINGS allow_experimental_analyzer=1;
