-- Row-wise input formats create the null map of a `Nullable` column only on the first NULL.
SELECT DISTINCT dumpColumnStructure(x) FROM format(CSV, 'x Nullable(UInt64)', '1\n2\n3');
SELECT DISTINCT dumpColumnStructure(x) FROM format(TSV, 'x Nullable(String)', 'a\nb');
SELECT DISTINCT dumpColumnStructure(x) FROM format(JSONEachRow, 'x Nullable(String)', '{"x":"a"}{"x":"b"}');
SELECT DISTINCT dumpColumnStructure(x) FROM format(Values, 'x Nullable(UInt64)', '(1),(2)');
SELECT x, isNull(x), dumpColumnStructure(x) FROM format(CSV, 'x Nullable(UInt64)', '1\n\\N\n3');
SELECT x, isNull(x), dumpColumnStructure(x) FROM format(JSONEachRow, 'x Nullable(String)', '{"x":"a"}{"x":null}{"x":"c"}');
SELECT x, isNull(x) FROM format(Values, 'x Nullable(UInt64)', '(NULL),(2)');
SELECT x FROM format(CSV, 'x Nullable(UInt64)', '1\nabc\n3') SETTINGS input_format_allow_errors_num = 1;

-- Conversion to `String` does not create a null map if there are no NULLs.
SELECT DISTINCT dumpColumnStructure(toString(toNullable(number))) FROM numbers(3);
SELECT DISTINCT dumpColumnStructure(toString(toNullable(toDateTime(number, 'UTC')))) FROM numbers(3);
SELECT DISTINCT dumpColumnStructure(toString(toNullable(toDecimal32(number, 2)))) FROM numbers(3);
SELECT DISTINCT dumpColumnStructure(CAST(toNullable(toFixedString(toString(number), 1)) AS Nullable(String))) FROM numbers(3);
SELECT toString(nullIf(number, 1)) AS s, isNull(s) FROM numbers(3);
