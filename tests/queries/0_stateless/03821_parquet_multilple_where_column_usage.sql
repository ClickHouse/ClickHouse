-- Tags: no-fasttest, no-parallel, no-replicated-database

insert into function file('03821_file.parquet') select number as x, number as y, number as z from numbers(10) settings engine_file_truncate_on_insert=1;
select * from file('03821_file.parquet') prewhere x > 5 and (x > 6 or y > 3);
