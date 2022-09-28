-- Tags: no-fasttest, no-parallel

insert into function file('02267_data2.jsonl') select NULL as x SETTINGS engine_file_truncate_on_insert = 1;
insert into function file('02267_data3.jsonl') select * from numbers(0) SETTINGS engine_file_truncate_on_insert = 1;
insert into function file('02267_data4.jsonl') select 1 as x SETTINGS engine_file_truncate_on_insert = 1;
select * from file('02267_data*.jsonl') order by x;

insert into function file('02267_data4.jsonl', 'TSV') select 1 as x;
insert into function file('02267_data1.jsonl', 'TSV') select [1,2,3] as x SETTINGS engine_file_truncate_on_insert = 1;

select * from file('02267_data*.jsonl') settings schema_inference_use_cache_for_file=0; --{serverError INCORRECT_DATA}
