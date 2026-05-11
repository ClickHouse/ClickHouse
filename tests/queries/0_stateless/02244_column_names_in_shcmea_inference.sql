-- Tags: no-fasttest, no-parallel

insert into function file('test_02244', 'TSV', 'x String, y UInt32') select 'Hello, world!', 42 settings engine_file_truncate_on_insert=1;
desc file('test_02244', 'TSV') settings column_names_for_schema_inference='x,y';

insert into function file('test_02244', 'CSV', 'x String, y UInt32') select 'Hello, world!', 42 settings engine_file_truncate_on_insert=1;
desc file('test_02244', 'CSV') settings column_names_for_schema_inference='x,y';

insert into function file('test_02244', 'JSONCompactEachRow', 'x String, y UInt32') select 'Hello, world!', 42 settings engine_file_truncate_on_insert=1;
desc file('test_02244', 'JSONCompactEachRow') settings column_names_for_schema_inference='x,y';

insert into function file('test_02244', 'Values', 'x String, y UInt32') select 'Hello, world!', 42 settings engine_file_truncate_on_insert=1;
desc file('test_02244', 'Values') settings column_names_for_schema_inference='x,y';

