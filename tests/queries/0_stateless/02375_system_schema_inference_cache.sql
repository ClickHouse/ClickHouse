set input_format_json_try_infer_numbers_from_strings=1;
insert into function file(currentDatabase() || '_02374_data1.jsonl') select number as x, 'str' as s from numbers(10);
insert into function file(currentDatabase() || '_02374_data2.jsonl') select number as x, 'str' as s from numbers(10);

system drop schema cache for file;

desc file(currentDatabase() || '_02374_data1.jsonl');
desc file(currentDatabase() || '_02374_data2.jsonl');

select storage, replace(splitByChar('/', source)[-1], currentDatabase() || '_', ''), format, schema from system.schema_inference_cache where storage='File' and position(source, '_02374_') > 0;
system drop schema cache for file;
select storage, source, format, schema from system.schema_inference_cache where storage='File';
