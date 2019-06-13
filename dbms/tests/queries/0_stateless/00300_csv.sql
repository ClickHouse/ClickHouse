SELECT 'Hello, "World"' AS x, 123 AS y, [1, 2, 3] AS z, (456, ['abc', 'def']) AS a, 'Newline\nhere' AS b FORMAT CSVWithNames;
SELECT 'Hello, "World"' AS x, 123 AS y, [1, 2, 3] AS z, (456, ['abc', 'def']) AS a, 'Newline\nhere' AS b FORMAT CSV;
SELECT number, toString(number), range(number), toDate('2000-01-01') + number, toDateTime('2000-01-01 00:00:00') + number FROM system.numbers LIMIT 10 FORMAT CSV;

-- Substitute default values for empty unquoted values in CSV
create temporary table def_test(a int default 7, b int);
set input_format_defaults_for_omitted_fields = 1;
insert into def_test format CSV ,1
select * from def_test;
