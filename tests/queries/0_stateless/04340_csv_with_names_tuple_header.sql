-- Tags: no-parallel
-- ^ uses a file in user_files for the round-trip check

-- Test for issue #107342: when output_format_csv_serialize_tuple_into_separate_columns is on
-- (the default), CSVWithNames / CSVWithNamesAndTypes flatten tuple VALUES into separate columns,
-- but the header used to keep only the top-level tuple name, so the header had fewer fields than
-- the data. output_format_csv_header_serialize_tuple_into_separate_columns (default 1) now flattens
-- the header (and the types row) the same way so the widths agree.

SELECT '-- CSVWithNames: header flattened to match data (default)';
SELECT
    (123::UInt64, 'Sophia')::Tuple(ID UInt64, Name String) AS User,
    (1::UInt16, 'Net1')::Tuple(ID UInt64, Name String) AS Network
FORMAT CSVWithNames;

SELECT '-- CSVWithNamesAndTypes: names and types rows both flattened (default)';
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CSVWithNamesAndTypes;

SELECT '-- opt-out: keep the single top-level tuple name and type';
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CSVWithNamesAndTypes
SETTINGS output_format_csv_header_serialize_tuple_into_separate_columns = 0;

SELECT '-- not flattened when values are not flattened either';
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CSVWithNames
SETTINGS output_format_csv_serialize_tuple_into_separate_columns = 0;

SELECT '-- nested tuples: recursive dotted names';
SELECT ((1::UInt8, (2::UInt8, 3::UInt8)), 'b')::Tuple(A Tuple(X UInt8, Y Tuple(P UInt8, Q UInt8)), B String) AS T
FORMAT CSVWithNames;

SELECT '-- unnamed tuple: positional element names';
SELECT (1::UInt8, 'a')::Tuple(UInt8, String) AS U
FORMAT CSVWithNames;

SELECT '-- mixed plain and tuple columns';
SELECT 5::UInt8 AS x, (1::UInt8, 'a')::Tuple(ID UInt8, Name String) AS U, 'z' AS y
FORMAT CSVWithNames;

SELECT '-- no tuples: header unchanged';
SELECT 1::UInt8 AS a, 'x' AS b
FORMAT CSVWithNames;

SELECT '-- empty tuple in a mixed row: still occupies one field, header keeps one cell';
SELECT 1::UInt8 AS x, tuple()::Tuple() AS t, 2::UInt8 AS y
FORMAT CSVWithNamesAndTypes;

SELECT '-- non-null Nullable(Tuple(...)): value flattens, so the header flattens the inner tuple too';
SELECT materialize(CAST((1, 'a'), 'Nullable(Tuple(ID UInt64, Name String))')) AS User
FORMAT CSVWithNamesAndTypes
SETTINGS allow_experimental_nullable_tuple_type = 1;

SELECT '-- scalar Nullable column: stays one cell with the full Nullable type name';
SELECT CAST(1, 'Nullable(UInt64)') AS n, 'x' AS s
FORMAT CSVWithNamesAndTypes;

-- CustomSeparated joins fields with format_custom_field_delimiter, but a tuple value uses
-- csv.tuple_delimiter (format_csv_delimiter, ',') internally. So the header may only be flattened
-- when the custom field delimiter is the same single character as the tuple delimiter; otherwise a
-- tuple value stays one custom field and a flattened header would have more fields than the data.
SELECT '-- CustomSeparatedWithNames, CSV rule, comma field delimiter: header flattened (delimiters match)';
SELECT 5::UInt8 AS x, (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CustomSeparatedWithNames
SETTINGS format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = ',';

SELECT '-- CustomSeparatedWithNamesAndTypes, CSV rule, comma field delimiter: names and types flattened';
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CustomSeparatedWithNamesAndTypes
SETTINGS format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = ',';

SELECT '-- CustomSeparatedWithNames, CSV rule, non-comma (|) field delimiter: header NOT flattened (matches one-field tuple value)';
SELECT 5::UInt8 AS x, (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CustomSeparatedWithNames
SETTINGS format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = '|';

SELECT '-- CustomSeparatedWithNames, CSV rule, default tab field delimiter: header NOT flattened';
SELECT 5::UInt8 AS x, (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CustomSeparatedWithNames
SETTINGS format_custom_escaping_rule = 'CSV';

SELECT '-- CustomSeparatedWithNames with non-CSV escaping rule: header not flattened';
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CustomSeparatedWithNames
SETTINGS format_custom_escaping_rule = 'Quoted', format_custom_field_delimiter = ',';

-- Round-trip: data written with a flattened header reads back into the tuple positionally
-- (input_format_with_names_use_header = 0), and the opt-out single-name header reads back by name.
SELECT '-- round-trip: opt-out single-name header reads back by name';
INSERT INTO FUNCTION file('04340_optout.csv', CSVWithNames)
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
SETTINGS engine_file_truncate_on_insert = 1, output_format_csv_header_serialize_tuple_into_separate_columns = 0;
SELECT * FROM file('04340_optout.csv', CSVWithNames, 'User Tuple(ID UInt64, Name String)');

SELECT '-- round-trip: flattened header reads back positionally (use_header = 0)';
INSERT INTO FUNCTION file('04340_flat.csv', CSVWithNames)
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
SETTINGS engine_file_truncate_on_insert = 1;
SELECT * FROM file('04340_flat.csv', CSVWithNames, 'User Tuple(ID UInt64, Name String)')
SETTINGS input_format_with_names_use_header = 0;
