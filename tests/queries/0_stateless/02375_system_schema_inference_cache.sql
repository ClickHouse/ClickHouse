-- Tags: no-fasttest, no-parallel

set input_format_json_try_infer_numbers_from_strings=1;
insert into function file('02374_data1.jsonl') select number as x, 'str' as s from numbers(10);
insert into function file('02374_data2.jsonl') select number as x, 'str' as s from numbers(10);

system drop schema cache for file;

desc file('02374_data1.jsonl');
desc file('02374_data2.jsonl');

select storage, splitByChar('/', source)[-1], format, schema from system.schema_inference_cache where storage='File';
system drop schema cache for file;
select storage, source, format, schema from system.schema_inference_cache where storage='File';
