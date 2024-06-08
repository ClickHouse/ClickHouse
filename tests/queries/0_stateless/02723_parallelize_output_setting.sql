-- Tags: no-parallel, no-fasttest

insert into function file(data_02723.csv) select number from numbers(5) settings engine_file_truncate_on_insert=1;

set max_threads=2;
-- { echoOn }
set parallelize_output_from_storages=1;
select startsWith(trimLeft(explain),'Resize') as resize from (explain pipeline select * from file(data_02723.csv)) where resize;
-- no Resize in pipeline
set parallelize_output_from_storages=0;
select startsWith(trimLeft(explain),'Resize') as resize from (explain pipeline select * from file(data_02723.csv)) where resize;

-- Data from URL source is immediately resized to max_treads streams, before any ExpressionTransform.
set parallelize_output_from_storages=1;
select match(arrayStringConcat(groupArray(explain), ''), '.*Resize 1 → 2 *URL 0 → 1 *$') from (explain pipeline select x, count() from url('https://example.com', Parquet, 'x Int64') group by x order by count() limit 10);