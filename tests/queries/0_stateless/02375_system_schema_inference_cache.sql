-- Tags: no-fasttest

insert into function file('02374_data1.jsonl') select number as x, 'str' as s from numbers(10);
insert into function file('02374_data2.jsonl') select number as x, 'str' as s from numbers(10);

desc system.schema_inference_cache;
system drop schema cache for file;

desc file('02374_data1.jsonl');
desc file('02374_data2.jsonl');

select storage, splitByChar('/', source)[-1], format, schema from system.schema_inference_cache where storage='File';
system drop schema cache for file;
select storage, source, format, schema from system.schema_inference_cache where storage='File';
