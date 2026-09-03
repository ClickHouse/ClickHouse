-- Tags: no-parallel, no-fasttest
set input_format_json_try_infer_numbers_from_strings=1;
select number, 'Hello' as str, range(number) as arr from numbers(3) format JSONObjectEachRow;
insert into function file(02417_data.jsonObjectEachRow) select number, 'Hello' as str, range(number) as arr from numbers(3) settings engine_file_truncate_on_insert=1;
desc file(02417_data.jsonObjectEachRow);
select * from file(02417_data.jsonObjectEachRow);

