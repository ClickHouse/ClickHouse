-- Tags: no-parallel

insert into function file(data_02723.csv) select number from numbers(5) settings engine_file_truncate_on_insert=1;
-- { echoOn }
select startsWith(trimLeft(explain),'Resize') as resize from (explain pipeline select * from file(data_02723.csv) settings parallelize_output_from_storages=1) where resize;
-- no Resize in pipeline
select startsWith(trimLeft(explain),'Resize') as resize from (explain pipeline select * from file(data_02723.csv) settings parallelize_output_from_storages=0) where resize;

