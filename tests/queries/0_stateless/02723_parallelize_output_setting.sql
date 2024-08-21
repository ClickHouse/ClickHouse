-- Tags: no-parallel

insert into function file(data_02723.csv) select number from numbers(5) settings engine_file_truncate_on_insert=1;

set max_threads=2;
-- { echoOn }
set parallelize_output_from_storages=1;
select startsWith(trimLeft(explain),'Resize') as resize from (explain pipeline select * from file(data_02723.csv)) where resize;
-- no Resize in pipeline
set parallelize_output_from_storages=0;
select startsWith(trimLeft(explain),'Resize') as resize from (explain pipeline select * from file(data_02723.csv)) where resize;

