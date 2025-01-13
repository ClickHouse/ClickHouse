SELECT toString((1, 'Hello', toDate('2016-01-01'))), toString([1, 2, 3]);
SELECT (number, toString(number), range(number)) AS x, toString(x) FROM system.numbers LIMIT 10;
SELECT hex(toString(countState())) FROM (SELECT * FROM system.numbers LIMIT 10);

SELECT CAST((1, 'Hello', toDate('2016-01-01')) AS String), CAST([1, 2, 3] AS String);
SELECT (number, toString(number), range(number)) AS x, CAST(x AS String) FROM system.numbers LIMIT 10;
SELECT hex(CAST(countState() AS String)) FROM (SELECT * FROM system.numbers LIMIT 10);

SELECT toDateTime64('2024-01-01 00:00:00.00', 6),
       cast(toDateTime64('2024-01-01 00:00:00.100', 6) as String),
       toString((1, toDateTime64('2024-01-01 00:00:00.12000', 6))),
       toString([toDateTime64('2024-01-01 00:00:00.123000', 6), toDateTime64('2024-01-01 00:00:00.123400', 6)]),
       JSONExtractString('{"a" : "2024-01-01 00:00:00"}', 'a')::DateTime64(6)
       SETTINGS date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = true;
