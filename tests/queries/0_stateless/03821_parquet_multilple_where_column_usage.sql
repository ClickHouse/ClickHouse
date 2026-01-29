insert into function file('t.parquet') select number as x, number as y, number as z from numbers(10) settings engine_file_truncate_on_insert=1;
select * from file('t.parquet') prewhere x > 5 and (x > 6 or y > 3);
