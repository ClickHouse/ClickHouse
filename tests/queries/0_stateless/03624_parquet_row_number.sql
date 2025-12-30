-- Tags: no-fasttest

set engine_file_truncate_on_insert = 1;

insert into function file(current_database() ||'03624_parquet_row_number.parquet') select number*10 as x from numbers(20) settings max_threads=1, output_format_parquet_row_group_size=5;

select _row_number, x from file(current_database() ||'03624_parquet_row_number.parquet') where x % 3 != 0 and x > 60 order by _row_number;
